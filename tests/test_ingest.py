import io
import json
from datetime import UTC, date, datetime
from urllib.parse import parse_qs, urlparse

import pytest

from energy_lakehouse import ingest


def test_export_url_filters_on_datetime_with_exclusive_end():
    url = ingest.build_export_url(date(2026, 10, 1), date(2026, 10, 2))
    query = parse_qs(urlparse(url).query)
    assert "/eco2mix-national-tr/exports/json" in url
    assert query["where"] == [
        "date_heure >= date'2026-10-01' AND date_heure < date'2026-10-03' AND consommation IS NOT NULL"
    ]
    assert query["timezone"] == ["UTC"]


def test_export_url_rejects_inverted_range():
    with pytest.raises(ValueError):
        ingest.build_export_url(date(2026, 10, 2), date(2026, 10, 1))


def test_landing_path_is_partitioned_by_ingest_date():
    path = ingest.landing_path("/Volumes/c/bronze/landing", datetime(2026, 10, 17, 9, 5, 0, tzinfo=UTC))
    assert str(path) == "/Volumes/c/bronze/landing/eco2mix/ingest_date=2026-10-17/eco2mix_20261017T090500Z.jsonl"


def test_fetch_retries_then_succeeds(monkeypatch):
    calls = []

    def fake_urlopen(request, timeout):
        calls.append(request.full_url)
        if len(calls) < 3:
            raise OSError("connection reset")
        return io.BytesIO(json.dumps([{"a": 1}]).encode())

    monkeypatch.setattr(ingest, "urlopen", fake_urlopen)
    monkeypatch.setattr(ingest.time, "sleep", lambda s: None)
    assert ingest.fetch_records("https://example.test") == [{"a": 1}]
    assert len(calls) == 3


def test_fetch_gives_up_after_retries(monkeypatch):
    def failing(request, timeout):
        raise OSError("down")

    monkeypatch.setattr(ingest, "urlopen", failing)
    monkeypatch.setattr(ingest.time, "sleep", lambda s: None)
    with pytest.raises(OSError):
        ingest.fetch_records("https://example.test", retries=2)


def test_run_writes_json_lines(monkeypatch, tmp_path, raw_records):
    monkeypatch.setattr(ingest, "fetch_records", lambda url: raw_records)
    target = ingest.run(str(tmp_path), lookback_days=1, today=date(2026, 10, 15))
    lines = target.read_text().splitlines()
    assert len(lines) == 96
    assert json.loads(lines[0])["date_heure"] == "2026-10-15T00:00:00+00:00"


def test_run_skips_empty_result(monkeypatch, tmp_path):
    monkeypatch.setattr(ingest, "fetch_records", lambda url: [])
    assert ingest.run(str(tmp_path)) is None
    assert list(tmp_path.iterdir()) == []
