from datetime import date, datetime, timedelta

import pytest

from energy_lakehouse.gold import daily_battery, daily_mix
from energy_lakehouse.silver import split_valid, to_silver

COLUMNS = [
    "ts",
    "consumption_mw",
    "nuclear_mw",
    "wind_mw",
    "solar_mw",
    "hydro_mw",
    "bioenergy_mw",
    "gas_mw",
    "coal_mw",
    "oil_mw",
    "net_exchanges_mw",
    "carbon_intensity_g_per_kwh",
    "battery_charge_mw",
    "battery_discharge_mw",
]


def synthetic_day(spark, local_day_start_utc, intervals=96):
    rows = [
        (
            local_day_start_utc + timedelta(minutes=15 * i),
            40_000.0,
            30_000.0,
            6_000.0,
            2_000.0,
            4_000.0,
            1_000.0,
            1_000.0,
            0.0,
            0.0,
            -4_000.0,
            20,
            -100.0,
            80.0,
        )
        for i in range(intervals)
    ]
    schema = (
        "ts timestamp, consumption_mw double, nuclear_mw double, wind_mw double, solar_mw double, hydro_mw double, "
        "bioenergy_mw double, gas_mw double, coal_mw double, oil_mw double, net_exchanges_mw double, "
        "carbon_intensity_g_per_kwh int, battery_charge_mw double, battery_discharge_mw double"
    )
    return spark.createDataFrame(rows, schema)


def test_daily_mix_energy_and_shares(spark):
    # 2026-06-01 00:00 Paris = 2026-05-31 22:00 UTC (CEST, UTC+2)
    day = daily_mix(synthetic_day(spark, datetime(2026, 5, 31, 22, 0))).collect()
    assert len(day) == 1
    row = day[0]
    assert row.day == date(2026, 6, 1)
    assert row.consumption_gwh == pytest.approx(960.0)  # 40 GW * 24 h
    assert row.nuclear_gwh == pytest.approx(720.0)
    assert row.generation_gwh == pytest.approx(1056.0)
    assert row.net_exports_gwh == pytest.approx(96.0)
    assert row.low_carbon_share == pytest.approx(round(1032 / 1056, 4))
    assert row.is_complete_day


def test_local_day_boundaries_follow_paris_time(spark):
    # Starting at UTC midnight in summer, the last 2 hours fall on the next local day.
    days = {r.day: r.intervals for r in daily_mix(synthetic_day(spark, datetime(2026, 6, 1, 0, 0))).collect()}
    assert days == {date(2026, 6, 1): 88, date(2026, 6, 2): 8}
    complete = {r.day: r.is_complete_day for r in daily_mix(synthetic_day(spark, datetime(2026, 6, 1))).collect()}
    assert complete == {date(2026, 6, 1): False, date(2026, 6, 2): False}


def test_daily_battery(spark):
    row = daily_battery(synthetic_day(spark, datetime(2026, 5, 31, 22, 0))).first()
    assert row.charged_mwh == pytest.approx(2_400.0)
    assert row.discharged_mwh == pytest.approx(1_920.0)
    assert row.discharge_to_charge_ratio == pytest.approx(0.8)
    assert row.peak_charge_mw == 100.0


def test_gold_on_real_data(raw_records, bronze_from_records):
    valid, _ = split_valid(to_silver(bronze_from_records(raw_records)))
    rows = {r.day: r for r in daily_mix(valid).collect()}
    # A UTC day spans two local days in October (UTC+2).
    assert set(rows) == {date(2026, 10, 15), date(2026, 10, 16)}
    full = rows[date(2026, 10, 15)]
    assert 0.5 < full.low_carbon_share <= 1.0
    assert 500 < full.consumption_gwh + rows[date(2026, 10, 16)].consumption_gwh < 2_500
