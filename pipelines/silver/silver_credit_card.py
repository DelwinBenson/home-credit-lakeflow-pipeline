import re
from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE = spark.conf.get("home_credit.bronze_schema")
SILVER = spark.conf.get("home_credit.silver_schema")

# ---- type contract (overrides first, then ordered rules; first match wins) ----
OVERRIDES = {
    "amt_credit_limit_actual": "int",
    "cnt_drawings_current": "int",
}
RULES = [(re.compile(p), t) for p, t in [
    (r"^(sk_id_|sk_dpd|months_balance$)", "int"),
    (r"^(amt_|cnt_)", "double"),
    (r"^name_", "string"),
]]
RENAMES = {"amt_recivable": "amt_receivable"}  # typo in the source


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


# ---- type, clean, flag ----
@dp.view(name="credit_card_balance_typed")
def credit_card_balance_typed():
    df = spark.readStream.table(f"{BRONZE}.credit_card_balance")
    src = [c for c in df.columns if not c.startswith("_")]
    return (
        df.select(
            *[cast_expr(c, resolve_type(c.lower())).alias(RENAMES.get(c.lower(), c.lower()))
              for c in src],
            F.col("SK_ID_PREV").alias("_raw_sk_id_prev"),
            F.col("SK_ID_CURR").alias("_raw_sk_id_curr"),
            F.col("MONTHS_BALANCE").alias("_raw_months_balance"),
            F.to_json(F.struct(*src)).alias("_raw_record"),
            F.col("_source_file"), F.col("_ingested_at"),
        )
        .withColumn("is_active", F.col("name_contract_status") == "Active")
        .withColumn("_failed_rule",
                    F.when(F.col("sk_id_prev").isNull(), "sk_id_prev missing or not an int")
                     .when(F.col("sk_id_curr").isNull(), "sk_id_curr missing or not an int")
                     .when(F.col("months_balance").isNull(), "months_balance missing or not an int"))
    )


AUDIT = ["_raw_sk_id_prev", "_raw_sk_id_curr", "_raw_months_balance", "_raw_record", "_failed_rule"]


# ---- silver table,one row per card per month (SCD type 1) ----
dp.create_streaming_table(
    name=f"{SILVER}.credit_card_balance",
    table_properties={"quality": "silver"},
    expect_all_or_drop={
        "key_present": "sk_id_prev IS NOT NULL AND sk_id_curr IS NOT NULL AND months_balance IS NOT NULL",
    },
    expect_all={
        "months_balance_in_past": "months_balance < 0",
        "dpd_non_negative": "sk_dpd IS NULL OR sk_dpd >= 0",
    },
)

dp.create_auto_cdc_flow(
    target=f"{SILVER}.credit_card_balance",
    source="credit_card_balance_typed",
    keys=["sk_id_prev", "months_balance"],
    sequence_by="_ingested_at",
    except_column_list=AUDIT,
    stored_as_scd_type=1,
)


# ---- bad rows ----
@dp.table(name=f"{SILVER}.credit_card_balance_quarantine", table_properties={"quality": "quarantine"})
def credit_card_balance_quarantine():
    return (spark.readStream.table("credit_card_balance_typed")
            .filter("_failed_rule IS NOT NULL")
            .select("_raw_sk_id_prev", "_raw_sk_id_curr", "_raw_months_balance", "_raw_record",
                    "_failed_rule", "_source_file", "_ingested_at"))