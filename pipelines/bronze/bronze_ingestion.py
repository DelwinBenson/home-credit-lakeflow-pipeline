from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("home_credit.landing_path")
TARGET = spark.conf.get("home_credit.target_schema")

BRONZE_TABLES = {
    "application_train": {
        "comment": "Raw customer applications",
        "expectations": {"valid_customer_key": "SK_ID_CURR IS NOT NULL"},
    },
    "previous_application": {
        "comment": "Raw previous loan applications",
        "expectations": {"valid_prev_key": "SK_ID_PREV IS NOT NULL"},
    },
    "bureau": {
        "comment": "Raw external credit bureau loans",
        "expectations": {"valid_bureau_key": "SK_ID_BUREAU IS NOT NULL"},
    },
    "credit_card_balance": {
        "comment": "Raw monthly credit card balances",
        "expectations": {
            "valid_prev_key": "SK_ID_PREV IS NOT NULL",
            "valid_month": "MONTHS_BALANCE IS NOT NULL",
        },
    },
}


def create_bronze_table(name, comment, expectations):
    @dp.table(
        name=f"{TARGET}.{name}",
        comment=comment,
        table_properties={"quality": "bronze"},
    )
    @dp.expect_all(expectations)
    def _bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("rescuedDataColumn", "_rescued_data")
            .load(f"{LANDING}/{name}/")
            .select(
                "*",
                F.col("_metadata.file_name").alias("_source_file"),
                F.current_timestamp().alias("_ingested_at"),
            )
        )


for table_name, cfg in BRONZE_TABLES.items():
    create_bronze_table(table_name, **cfg)