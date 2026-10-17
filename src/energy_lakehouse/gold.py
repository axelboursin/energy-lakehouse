"""Gold: daily business aggregates, computed in local French time.

Gold is small (one row per day), so it is fully recomputed from silver on each
run. That keeps it idempotent and makes late corrections in silver flow through.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from energy_lakehouse.config import Paths
from energy_lakehouse.schema import GENERATION_SOURCES, INTERVAL_HOURS, LOW_CARBON_SOURCES

LOCAL_TZ = "Europe/Paris"
# A local day has 92, 96 or 100 quarter-hours (DST changes).
MIN_INTERVALS_FOR_COMPLETE_DAY = 92


def _local_day(df: DataFrame) -> DataFrame:
    return df.withColumn("day", F.to_date(F.from_utc_timestamp("ts", LOCAL_TZ)))


def _gwh(column: str) -> Column:
    return F.sum(F.coalesce(F.col(column), F.lit(0.0))) * INTERVAL_HOURS / 1000


def daily_mix(silver: DataFrame) -> DataFrame:
    source_energy = [_gwh(c).alias(c.replace("_mw", "_gwh")) for c in GENERATION_SOURCES]
    daily = (
        _local_day(silver)
        .groupBy("day")
        .agg(
            _gwh("consumption_mw").alias("consumption_gwh"),
            F.max("consumption_mw").alias("peak_consumption_mw"),
            *source_energy,
            (-_gwh("net_exchanges_mw")).alias("net_exports_gwh"),
            F.round(F.avg("carbon_intensity_g_per_kwh"), 1).alias("avg_carbon_intensity_g_per_kwh"),
            F.max("carbon_intensity_g_per_kwh").alias("max_carbon_intensity_g_per_kwh"),
            F.count("*").alias("intervals"),
        )
    )
    generation = sum((F.col(c.replace("_mw", "_gwh")) for c in GENERATION_SOURCES), F.lit(0.0))
    low_carbon = sum((F.col(c.replace("_mw", "_gwh")) for c in LOW_CARBON_SOURCES), F.lit(0.0))
    return (
        daily.withColumn("generation_gwh", generation)
        .withColumn(
            "low_carbon_share",
            F.when(F.col("generation_gwh") > 0, F.round(low_carbon / F.col("generation_gwh"), 4)),
        )
        .withColumn("is_complete_day", F.col("intervals") >= MIN_INTERVALS_FOR_COMPLETE_DAY)
        .orderBy("day")
    )


def daily_battery(silver: DataFrame) -> DataFrame:
    """Grid-scale battery activity in France: the market this kind of asset trades in."""
    return (
        _local_day(silver)
        .groupBy("day")
        .agg(
            (F.sum(F.abs(F.coalesce("battery_charge_mw", F.lit(0.0)))) * INTERVAL_HOURS).alias("charged_mwh"),
            (F.sum(F.coalesce("battery_discharge_mw", F.lit(0.0))) * INTERVAL_HOURS).alias("discharged_mwh"),
            F.max("battery_discharge_mw").alias("peak_discharge_mw"),
            F.max(F.abs("battery_charge_mw")).alias("peak_charge_mw"),
        )
        .withColumn(
            "discharge_to_charge_ratio",
            F.when(F.col("charged_mwh") > 0, F.round(F.col("discharged_mwh") / F.col("charged_mwh"), 3)),
        )
        .orderBy("day")
    )


def run(spark: SparkSession, paths: Paths) -> None:
    silver = spark.read.table(paths.silver_table)
    daily_mix(silver).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(paths.gold_daily_mix)
    daily_battery(silver).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        paths.gold_daily_battery
    )
