"""Quality gate: fail the job run (and alert) when data is stale or too dirty.

Runs last in the job. The checks are SLO-style thresholds on the last 24 hours,
so a single bad record never pages anyone but a broken feed does.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from energy_lakehouse.config import Paths

log = logging.getLogger(__name__)

MAX_STALENESS = timedelta(hours=3)
MAX_QUARANTINE_RATIO = 0.05


@dataclass(frozen=True)
class QualityMetrics:
    latest_ts: datetime | None
    valid_rows_24h: int
    quarantined_rows_24h: int
    now: datetime


class QualityGateError(RuntimeError):
    pass


def evaluate(metrics: QualityMetrics) -> list[str]:
    """Return the list of violated checks (empty when healthy)."""
    violations = []
    if metrics.latest_ts is None:
        violations.append("silver table is empty")
    elif metrics.now - metrics.latest_ts > MAX_STALENESS:
        violations.append(
            f"freshness: latest interval {metrics.latest_ts:%Y-%m-%d %H:%M} UTC is older than {MAX_STALENESS}"
        )

    total = metrics.valid_rows_24h + metrics.quarantined_rows_24h
    if total and metrics.quarantined_rows_24h / total > MAX_QUARANTINE_RATIO:
        violations.append(f"quarantine ratio {metrics.quarantined_rows_24h}/{total} exceeds {MAX_QUARANTINE_RATIO:.0%}")
    return violations


def collect_metrics(spark: SparkSession, paths: Paths, now: datetime) -> QualityMetrics:
    since = now - timedelta(hours=24)
    silver = spark.read.table(paths.silver_table)
    latest_ts = silver.agg(F.max("ts")).first()[0]
    valid = silver.where(F.col("_ingested_at") >= F.lit(since)).count()
    quarantined = (
        spark.read.table(paths.quarantine_table).where(F.col("_quarantined_at") >= F.lit(since)).count()
        if spark.catalog.tableExists(paths.quarantine_table)
        else 0
    )
    return QualityMetrics(latest_ts, valid, quarantined, now)


def run(spark: SparkSession, paths: Paths) -> None:
    now = spark.sql("SELECT current_timestamp()").first()[0]
    metrics = collect_metrics(spark, paths, now)
    log.info("Quality metrics: %s", metrics)
    violations = evaluate(metrics)
    if violations:
        raise QualityGateError("; ".join(violations))
