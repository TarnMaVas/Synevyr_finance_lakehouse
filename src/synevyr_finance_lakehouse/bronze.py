from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def load_source_table(
    spark: SparkSession,
    source_table_name: str,
) -> DataFrame:
    return spark.table(source_table_name)


def add_ingestion_metadata(
    df: DataFrame,
    source_table_name: str,
) -> DataFrame:
    return (
        df
        .withColumn(
            "_ingested_at",
            F.current_timestamp(),
        )
        .withColumn(
            "_source_table",
            F.lit(source_table_name),
        )
    )


def write_bronze_table(
    df: DataFrame,
    target_table_name: str,
) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table_name)
    )


def ingest_to_bronze(
    spark: SparkSession,
    source_table_name: str,
    target_table_name: str,
) -> None:
    source_df = load_source_table(
        spark,
        source_table_name,
    )

    bronze_df = add_ingestion_metadata(
        source_df,
        source_table_name,
    )

    write_bronze_table(
        bronze_df,
        target_table_name,
    )