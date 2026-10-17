import json
import os
import sys
import time
from pathlib import Path

# Make results independent of the machine running the tests:
# - PySpark converts collected timestamps with the *process* time zone;
# - Spark workers must use this interpreter, not whatever `python3` is on PATH.
os.environ["TZ"] = "UTC"
time.tzset()
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pytest
from pyspark.sql import SparkSession

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("energy-lakehouse-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def raw_records():
    """One real UTC day (96 quarter-hours) from the eCO2mix open-data API."""
    lines = (FIXTURES / "eco2mix_2026-10-15.jsonl").read_text().splitlines()
    return [json.loads(line) for line in lines]


@pytest.fixture
def bronze_from_records(spark, tmp_path):
    """Read records the way bronze does: every value kept as a string."""
    from energy_lakehouse.bronze import add_ingestion_metadata

    def build(records):
        path = tmp_path / f"landing_{len(list(tmp_path.iterdir()))}.jsonl"
        path.write_text("\n".join(json.dumps(r) for r in records) + "\n")
        df = spark.read.option("primitivesAsString", "true").json(str(path))
        return add_ingestion_metadata(df)

    return build
