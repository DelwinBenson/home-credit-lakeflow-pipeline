import re
from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE = spark.conf.get("home_credit.bronze_schema")
SILVER = spark.conf.get("home_credit.silver_schema")

# ---- type contract (overrides first, then ordered rules; first match wins) ----
OVERRIDES = {
    "hour_appr_process_start": "int",
    "days_decision": "int",
    "sellerplace_area": "int",
}
RULES = [(re.compile(p), t) for p, t in [
    (r"^sk_id_", "int"),
    (r"^(flag_|nflag_)", "boolean"),
    (r"^(amt_|rate_|days_|cnt_)", "double"),
    (r"^(name_|code_|weekday_|channel_type$|product_combination$)", "string"),
]]
# only columns where these placeholders were verified in the data
XNA_TO_NULL = ["name_payment_type", "name_cash_loan_purpose", "name_goods_category",
               "name_seller_industry", "name_product_type", "name_yield_group",
               "code_reject_reason"]
SENTINEL_DAYS = ["days_first_drawing", "days_first_due", "days_last_due_1st_version",
                 "days_last_due", "days_termination"]


def resolve_type(col):
    if col in OVERRIDES:
        return OVERRIDES[col]
    for pattern, dtype in RULES:
        if pattern.match(col):
            return dtype
    raise ValueError(f"No type rule for column: {col}")


def cast_expr(col, dtype):
    c = F.col(col)
    if dtype == "boolean":
        return (F.when(c.isin("1", "1.0", "Y"), True)
                 .when(c.isin("0", "0.0", "N"), False))
    if dtype == "string":
        return F.trim(c)
    return F.expr(f"try_cast(`{col}` AS {dtype})")


# ---- step 1: type, clean, flag ----
@dp.view(name="previous_application_typed")
def previous_application_typed():
    df = spark.readStream.table(f"{BRONZE}.previous_application")
    src = [c for c in df.columns if not c.startswith("_")]
    typed = df.select(
        *[cast_expr(c, resolve_type(c.lower())).alias(c.lower()) for c in src],
        F.col("SK_ID_PREV").alias("_raw_sk_id_prev"),
        F.col("SK_ID_CURR").alias("_raw_sk_id_curr"),
        F.to_json(F.struct(*src)).alias("_raw_record"),
        F.col("_source_file"), F.col("_ingested_at"),
    )
    for c in XNA_TO_NULL:
        typed = typed.withColumn(c, F.when(F.col(c) == "XNA", None).otherwise(F.col(c)))
    for c in SENTINEL_DAYS:
        typed = typed.withColumn(c, F.when(F.col(c) == 365243, None).otherwise(F.col(c)))
    return (
        typed
        .withColumn("is_approved", F.col("name_contract_status") == "Approved")
        .withColumn("is_refused", F.col("name_contract_status") == "Refused")
        .withColumn("_failed_rule",
                    F.when(F.col("sk_id_prev").isNull(), "sk_id_prev missing or not an int")
                     .when(F.col("sk_id_curr").isNull(), "sk_id_curr missing or not an int"))
    )


AUDIT = ["_raw_sk_id_prev", "_raw_sk_id_curr", "_raw_record", "_failed_rule"]


# ---- step 2: silver table = one row per previous application (SCD type 1) ----
dp.create_streaming_table(
    name=f"{SILVER}.previous_application",
    table_properties={"quality": "silver"},
    expect_all_or_drop={
        "key_present": "sk_id_prev IS NOT NULL AND sk_id_curr IS NOT NULL",
    },
    expect_all={
        "status_known": "name_contract_status IN ('Approved','Refused','Canceled','Unused offer')",
        "decision_in_past": "days_decision IS NULL OR days_decision < 0",  # count not profiled yet
    },
)

dp.create_auto_cdc_flow(
    target=f"{SILVER}.previous_application",
    source="previous_application_typed",
    keys=["sk_id_prev"],
    sequence_by="_ingested_at",
    except_column_list=AUDIT,
    stored_as_scd_type=1,
)


# ---- step 3: bad rows ----
@dp.table(name=f"{SILVER}.previous_application_quarantine",
          table_properties={"quality": "quarantine"})
def previous_application_quarantine():
    return (spark.readStream.table("previous_application_typed")
            .filter("_failed_rule IS NOT NULL")
            .select("_raw_sk_id_prev", "_raw_sk_id_curr", "_raw_record",
                    "_failed_rule", "_source_file", "_ingested_at"))