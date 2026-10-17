"""Silver: typed, validated, deduplicated 15-minute records.

- Every value is cast with try_cast: a malformed value becomes NULL and is
  reported in `_parse_errors` instead of failing the whole batch (Spark 4 runs
  in ANSI mode, where a plain cast would raise).
- Rows failing a quality rule go to a quarantine table with the rule names.
- Valid rows are upserted on the interval timestamp, newest ingestion wins.
"""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from energy_lakehouse.config import Paths
from energy_lakehouse.schema import POWER_COLUMNS

# rule name -> condition that must be TRUE for a valid row (NULL counts as failed)
QUALITY_RULES: dict[str, Callable[[], Column]] = {
    "timestamp_present": lambda: F.col("ts").isNotNull(),
    "consumption_present": lambda: F.col("consumption_mw").isNotNull(),
    "consumption_plausible": lambda: F.col("consumption_mw").between(10_000, 110_000),
    "carbon_intensity_plausible": lambda: F.col("carbon_intensity_g_per_kwh").between(0, 1_000),
    "values_parsable": lambda: F.size("_parse_errors") == 0,
}


def _try_cast(column: str, data_type: str) -> Column:
    return F.expr(f"try_cast(`{column}` AS {data_type})")


def to_silver(bronze: DataFrame) -> DataFrame:
    """Rename and type bronze columns. Columns absent from the source become NULL."""
    present = set(bronze.columns)
    typed: list[Column] = [
        _try_cast("date_heure", "timestamp").alias("ts"),
        _try_cast("taux_co2", "int").alias("carbon_intensity_g_per_kwh")
        if "taux_co2" in present
        else F.lit(None).cast("int").alias("carbon_intensity_g_per_kwh"),
    ]
    parse_errors: list[Column] = []
    for source, target in POWER_COLUMNS.items():
        if source in present:
            typed.append(_try_cast(source, "double").alias(target))
            parse_errors.append(F.when(F.col(source).isNotNull() & _try_cast(source, "double").isNull(), F.lit(source)))
        else:
            typed.append(F.lit(None).cast("double").alias(target))

    return bronze.select(
        *typed,
        F.array_compact(F.array(*parse_errors)).alias("_parse_errors")
        if parse_errors
        else F.array().cast("array<string>").alias("_parse_errors"),
        "_ingested_at",
        "_source_file",
    )


def with_failed_rules(df: DataFrame) -> DataFrame:
    checks = [F.when(~F.coalesce(rule(), F.lit(False)), F.lit(name)) for name, rule in QUALITY_RULES.items()]
    return df.withColumn("_failed_rules", F.array_compact(F.array(*checks)))


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    checked = with_failed_rules(df)
    valid = checked.where(F.size("_failed_rules") == 0).drop("_failed_rules")
    quarantine = checked.where(F.size("_failed_rules") > 0).withColumn("_quarantined_at", F.current_timestamp())
    return valid, quarantine


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep the most recently ingested version of each 15-minute interval."""
    latest = Window.partitionBy("ts").orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
    return df.withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn")


def _upsert_batch(spark: SparkSession, paths: Paths) -> Callable[[DataFrame, int], None]:
    from delta.tables import DeltaTable  # available on Databricks runtimes

    def upsert(batch: DataFrame, batch_id: int) -> None:
        valid, quarantine = split_valid(to_silver(batch))
        quarantine.write.mode("append").option("mergeSchema", "true").saveAsTable(paths.quarantine_table)

        valid = deduplicate(valid).drop("_parse_errors")
        if not spark.catalog.tableExists(paths.silver_table):
            valid.write.clusterBy("ts").saveAsTable(paths.silver_table)
            return
        (
            DeltaTable.forName(spark, paths.silver_table)
            .alias("t")
            .merge(valid.alias("s"), "t.ts = s.ts")
            .whenMatchedUpdateAll(condition="s._ingested_at > t._ingested_at")
            .whenNotMatchedInsertAll()
            .execute()
        )

    return upsert


def run(spark: SparkSession, paths: Paths) -> None:
    (
        spark.readStream.table(paths.bronze_table)
        .writeStream.foreachBatch(_upsert_batch(spark, paths))
        .option("checkpointLocation", paths.checkpoint("silver"))
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )
