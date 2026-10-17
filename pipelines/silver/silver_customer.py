import re
from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE = spark.conf.get("home_credit.bronze_schema")
SILVER = spark.conf.get("home_credit.silver_schema")

# ---- type contract (overrides first, then ordered rules; first match wins) ----
OVERRIDES = {
    "own_car_age": "double", "cnt_fam_members": "double",
    "fondkapremont_mode": "string", "housetype_mode": "string",
    "wallsmaterial_mode": "string", "emergencystate_mode": "string",
}
RULES = [(re.compile(p), t) for p, t in [
    (r"^(sk_id_|target$|cnt_children$|hour_appr|region_rating_)", "int"),
    (r"^(flag_|(reg|live)_(region|city)_not_)", "boolean"),
    (r"^(name_|code_|occupation_type$|organization_type$|weekday_)", "string"),
    (r"^(amt_|days_|ext_source_|obs_|def_|region_population|totalarea)", "double"),
    (r".*_(avg|mode|medi)$", "double"),
]]

XNA_TO_NULL = ["code_gender", "organization_type"]


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
        return F.when(c.isin("1", "Y"), True).when(c.isin("0", "N"), False)
    if dtype == "string":
        return F.trim(c)
    return F.expr(f"try_cast(`{col}` AS {dtype})")


# ---- type, clean, and flag bad rows (shared by both outputs) ----
@dp.view(name="customer_typed")
def customer_typed():
    df = spark.readStream.table(f"{BRONZE}.application_train")
    src = [c for c in df.columns if not c.startswith("_")]
    typed = df.select(
        *[cast_expr(c, resolve_type(c.lower())).alias(c.lower()) for c in src],
        F.col("SK_ID_CURR").alias("_raw_sk_id_curr"),
        F.to_json(F.struct(*src)).alias("_raw_record"),
        F.col("_source_file"), F.col("_ingested_at"),
    )
    for c in XNA_TO_NULL:
        typed = typed.withColumn(c, F.when(F.col(c) == "XNA", None).otherwise(F.col(c)))
    return (
        typed
        .withColumn("is_employed", F.col("days_employed") != 365243)
        .withColumn("days_employed",
                    F.when(F.col("days_employed") == 365243, None).otherwise(F.col("days_employed")))
        .withColumn("age_years", F.floor(-F.col("days_birth") / 365.25))
        .withColumn("years_employed", F.round(-F.col("days_employed") / 365.25, 1))
        .withColumn("_failed_rule",
                    F.when(F.col("sk_id_curr").isNull(), "sk_id_curr missing or not an int")
                     .when(F.col("target").isNull(), "target missing or not an int"))
    )


AUDIT = ["_raw_sk_id_curr", "_raw_record", "_failed_rule"]


# ---- good rows (expectations here) ----
@dp.view(name="customer_valid")
@dp.expect_all_or_drop({"key_present": "sk_id_curr IS NOT NULL"})
@dp.expect_all({"ext_source_2_in_range": "ext_source_2 IS NULL OR ext_source_2 BETWEEN 0 AND 1"})
def customer_valid():
    return (spark.readStream.table("customer_typed")
            .filter("_failed_rule IS NULL")
            .drop(*AUDIT))


# ---- silver customer = current state, one row per key (SCD type 1) ----
dp.create_streaming_table(
    name=f"{SILVER}.customer",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target=f"{SILVER}.customer",
    source="customer_valid",
    keys=["sk_id_curr"],
    sequence_by="_ingested_at",
    stored_as_scd_type=1,
)


# ---- bad rows ----
@dp.table(name=f"{SILVER}.customer_quarantine", table_properties={"quality": "quarantine"})
def customer_quarantine():
    return (spark.readStream.table("customer_typed")
            .filter("_failed_rule IS NOT NULL")
            .select("_raw_sk_id_curr", "_raw_record", "_failed_rule",
                    "_source_file", "_ingested_at"))