"""Bronze: incremental, schema-tolerant load of landed files with Auto Loader.

Every column is kept as a string (`inferColumnTypes=false`): the source has
been seen sending numbers as strings, and bronze must never drop a record
because of a type change. Typing happens in silver.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from energy_lakehouse.config import Paths


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_source_file", F.col("_metadata.file_path"))


def run(spark: SparkSession, paths: Paths) -> None:
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", paths.checkpoint("bronze_schema"))
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(paths.landing_eco2mix)
    )
    (
        add_ingestion_metadata(stream)
        .writeStream.option("checkpointLocation", paths.checkpoint("bronze"))
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(paths.bronze_table)
        .awaitTermination()
    )
