import re
from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE = spark.conf.get("home_credit.bronze_schema")
SILVER = spark.conf.get("home_credit.silver_schema")

MAX_DAYS = 36500  # ~100 years

# ---- type contract (overrides first,then ordered rules)----
OVERRIDES = {
    "days_credit_enddate": "double",
    "days_enddate_fact": "double",
}
RULES = [(re.compile(p), t) for p, t in [
    (r"^(sk_id_|credit_day_overdue$|cnt_credit_prolong$|days_credit$|days_credit_update$)", "int"),
    (r"^amt_", "double"),
    (r"^credit_", "string"),
]]


def resolve_type(col):
    if col in OVERRIDES:
        return OVERRIDES[col]
    for pattern, dtype in RULES:
        if pattern.match(col):
            return dtype
    raise ValueError(f"No type rule for column: {col}")


def cast_expr(col, dtype):
    c = F.col(col)
    if dtype == "string":
        return F.trim(c)
    return F.expr(f"try_cast(`{col}` AS {dtype})")


def bad_end_date(col):
    """True if the end date is >100 years away OR earlier than the loan's start."""
    return F.coalesce(
        (F.abs(F.col(col)) > MAX_DAYS) | (F.col(col) < F.col("days_credit")),
        F.lit(False),
    )


# ---- type, clean, flag ----
@dp.view(name="bureau_loan_typed")
def bureau_loan_typed():
    df = spark.readStream.table(f"{BRONZE}.bureau")
    src = [c for c in df.columns if not c.startswith("_")]
    typed = df.select(
        *[cast_expr(c, resolve_type(c.lower())).alias(c.lower()) for c in src],
        F.col("SK_ID_BUREAU").alias("_raw_sk_id_bureau"),
        F.col("SK_ID_CURR").alias("_raw_sk_id_curr"),
        F.to_json(F.struct(*src)).alias("_raw_record"),
        F.col("_source_file"), F.col("_ingested_at"),
    )
    return (
        typed
        # decide which end dates are impossible BEFORE nulling anything
        .withColumn("_sched_bad", bad_end_date("days_credit_enddate"))
        .withColumn("_fact_bad", bad_end_date("days_enddate_fact"))
        .withColumn("end_date_nulled", F.col("_sched_bad") | F.col("_fact_bad"))
        .withColumn("days_credit_enddate",
                    F.when(F.col("_sched_bad"), None).otherwise(F.col("days_credit_enddate")))
        .withColumn("days_enddate_fact",
                    F.when(F.col("_fact_bad"), None).otherwise(F.col("days_enddate_fact")))
        .drop("_sched_bad", "_fact_bad")
        # helper flags for gold
        .withColumn("is_active", F.col("credit_active") == "Active")
        .withColumn("is_bad_debt", F.col("credit_active") == "Bad debt")
        .withColumn("is_primary_currency", F.col("credit_currency") == "currency 1")
        .withColumn("status_date_conflict",
                    F.coalesce((F.col("credit_active") == "Active")
                               & F.col("days_enddate_fact").isNotNull(), F.lit(False)))
        .withColumn("_failed_rule",
                    F.when(F.col("sk_id_bureau").isNull(), "sk_id_bureau missing or not an int")
                     .when(F.col("sk_id_curr").isNull(), "sk_id_curr missing or not an int"))
    )


AUDIT = ["_raw_sk_id_bureau", "_raw_sk_id_curr", "_raw_record", "_failed_rule"]


# ---- silver table,one row per loan (SCD type 1) ----
dp.create_streaming_table(
    name=f"{SILVER}.bureau_loan",
    table_properties={"quality": "silver"},
    expect_all_or_drop={
        "key_present": "sk_id_bureau IS NOT NULL AND sk_id_curr IS NOT NULL",
    },
    expect_all={
        "sched_end_in_range": "days_credit_enddate IS NULL OR days_credit_enddate BETWEEN -36500 AND 36500",
        "sched_end_after_start": "days_credit_enddate IS NULL OR days_credit IS NULL OR days_credit_enddate >= days_credit",
        "actual_end_in_range": "days_enddate_fact IS NULL OR days_enddate_fact BETWEEN -36500 AND 36500",
        "actual_end_after_start": "days_enddate_fact IS NULL OR days_credit IS NULL OR days_enddate_fact >= days_credit",
        # real source quirks: counted, rows kept
        "active_has_no_actual_end": "credit_active != 'Active' OR days_enddate_fact IS NULL",
        "credit_sum_plausible": "amt_credit_sum IS NULL OR amt_credit_sum <= 100000000",
    },
)

dp.create_auto_cdc_flow(
    target=f"{SILVER}.bureau_loan",
    source="bureau_loan_typed",
    keys=["sk_id_bureau"],
    sequence_by="_ingested_at",
    except_column_list=AUDIT,
    stored_as_scd_type=1,
)


# ---- bad rows ----
@dp.table(name=f"{SILVER}.bureau_loan_quarantine", table_properties={"quality": "quarantine"})
def bureau_loan_quarantine():
    return (spark.readStream.table("bureau_loan_typed")
            .filter("_failed_rule IS NOT NULL")
            .select("_raw_sk_id_bureau", "_raw_sk_id_curr", "_raw_record",
                    "_failed_rule", "_source_file", "_ingested_at"))








