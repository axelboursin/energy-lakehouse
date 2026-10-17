from datetime import datetime, timedelta

from energy_lakehouse.quality import QualityMetrics, evaluate

NOW = datetime(2026, 10, 17, 12, 0)


def metrics(latest=NOW - timedelta(minutes=45), valid=96, quarantined=0):
    return QualityMetrics(latest_ts=latest, valid_rows_24h=valid, quarantined_rows_24h=quarantined, now=NOW)


def test_healthy_feed_passes():
    assert evaluate(metrics()) == []


def test_stale_feed_fails():
    [violation] = evaluate(metrics(latest=NOW - timedelta(hours=4)))
    assert violation.startswith("freshness")


def test_empty_silver_fails():
    assert evaluate(metrics(latest=None, valid=0)) == ["silver table is empty"]


def test_quarantine_ratio_threshold():
    assert evaluate(metrics(valid=95, quarantined=5)) == []  # exactly 5% is tolerated
    [violation] = evaluate(metrics(valid=90, quarantined=10))
    assert "quarantine ratio 10/100" in violation
