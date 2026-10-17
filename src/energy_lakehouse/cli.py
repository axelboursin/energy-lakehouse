"""Entry point for the Databricks job tasks (python_wheel_task, entry point `lakehouse`).

lakehouse ingest --catalog energy_dev [--lookback-days 2]
lakehouse bronze --catalog energy_dev
lakehouse silver --catalog energy_dev
lakehouse gold   --catalog energy_dev
lakehouse quality --catalog energy_dev
"""

from __future__ import annotations

import argparse
import logging
import sys

from energy_lakehouse import bronze, gold, ingest, quality, silver
from energy_lakehouse.config import Paths


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="lakehouse")
    parser.add_argument("step", choices=["ingest", "bronze", "silver", "gold", "quality"])
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog of the target environment")
    parser.add_argument("--lookback-days", type=int, default=2)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    args = parse_args(sys.argv[1:] if argv is None else argv)
    paths = Paths(args.catalog)

    if args.step == "ingest":
        ingest.run(paths.landing_volume, lookback_days=args.lookback_days)
        return

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    # Gold derives local French days from UTC instants; pin the session zone.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    steps = {"bronze": bronze.run, "silver": silver.run, "gold": gold.run, "quality": quality.run}
    steps[args.step](spark, paths)


if __name__ == "__main__":
    main()
