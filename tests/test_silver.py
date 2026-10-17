import copy
from datetime import UTC, datetime

from pyspark.sql import functions as F

from energy_lakehouse.silver import deduplicate, split_valid, to_silver


def test_real_day_is_fully_valid(raw_records, bronze_from_records):
    valid, quarantine = split_valid(to_silver(bronze_from_records(raw_records)))
    assert valid.count() == 96
    assert quarantine.count() == 0


def test_types_and_string_encoded_numbers(raw_records, bronze_from_records):
    # The API sends hydraulique_fil_eau_eclusee as a JSON string: it must still parse.
    assert isinstance(raw_records[0]["hydraulique_fil_eau_eclusee"], str)
    silver = to_silver(bronze_from_records(raw_records[:1]))
    types = dict(silver.dtypes)
    assert types["ts"] == "timestamp"
    assert types["consumption_mw"] == "double"
    assert types["carbon_intensity_g_per_kwh"] == "int"
    row = silver.first()
    assert row.ts == datetime(2026, 10, 15, 0, 0)
    assert row.hydro_run_of_river_mw == float(raw_records[0]["hydraulique_fil_eau_eclusee"])
    assert row._parse_errors == []


def test_unparsable_and_implausible_rows_are_quarantined(raw_records, bronze_from_records):
    records = copy.deepcopy(raw_records[:4])
    records[1]["nucleaire"] = "ND"  # not a number
    records[2]["consommation"] = 500  # implausible for France
    records[3]["date_heure"] = "not-a-date"
    valid, quarantine = split_valid(to_silver(bronze_from_records(records)))

    assert valid.count() == 1
    failed = {
        row.consumption_mw: (row._failed_rules, row._parse_errors)
        for row in quarantine.select("consumption_mw", "_failed_rules", "_parse_errors").collect()
    }
    assert failed[float(records[1]["consommation"])] == (["values_parsable"], ["nucleaire"])
    assert failed[500.0] == (["consumption_plausible"], [])
    assert "timestamp_present" in failed[float(records[3]["consommation"])][0]


def test_missing_source_column_becomes_null(raw_records, bronze_from_records):
    records = copy.deepcopy(raw_records[:1])
    del records[0]["destockage_batterie"]
    silver = to_silver(bronze_from_records(records).drop("destockage_batterie"))
    assert silver.first().battery_discharge_mw is None


def test_deduplicate_keeps_latest_ingestion(raw_records, bronze_from_records):
    old = to_silver(bronze_from_records(raw_records[:2])).withColumn(
        "_ingested_at", F.lit(datetime(2026, 10, 15, 1, tzinfo=UTC))
    )
    corrected = copy.deepcopy(raw_records[:1])
    corrected[0]["consommation"] = 99_999
    new = to_silver(bronze_from_records(corrected)).withColumn(
        "_ingested_at", F.lit(datetime(2026, 10, 16, 1, tzinfo=UTC))
    )
    result = {r.ts: r.consumption_mw for r in deduplicate(old.unionByName(new)).collect()}
    assert len(result) == 2
    assert result[datetime(2026, 10, 15, 0, 0)] == 99_999.0
