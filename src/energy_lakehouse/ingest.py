"""Landing: download eCO2mix records from the ODRE API into a Unity Catalog volume.

Records are written untouched as JSON Lines, one file per run, so the raw
payload is always replayable. Auto Loader picks them up in the bronze step.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

log = logging.getLogger(__name__)

API_ROOT = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
DATASET = "eco2mix-national-tr"


def build_export_url(start: date, end: date, dataset: str = DATASET) -> str:
    """Export endpoint returns every matching record (no 10k pagination cap)."""
    if end < start:
        raise ValueError("end must be on or after start")
    # `date` is a text field in this dataset; filter on the `date_heure` datetime.
    end_exclusive = end + timedelta(days=1)
    where = (
        f"date_heure >= date'{start.isoformat()}' AND date_heure < date'{end_exclusive.isoformat()}'"
        " AND consommation IS NOT NULL"
    )
    query = urlencode({"where": where, "order_by": "date_heure", "timezone": "UTC"})
    return f"{API_ROOT}/{dataset}/exports/json?{query}"


def fetch_records(url: str, retries: int = 4, backoff_s: float = 2.0, timeout_s: float = 60.0) -> list[dict]:
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "energy-lakehouse/0.1"})  # noqa: S310
    for attempt in range(1, retries + 1):
        try:
            with urlopen(request, timeout=timeout_s) as response:  # noqa: S310 (fixed https URL)
                records = json.load(response)
            if not isinstance(records, list):
                raise ValueError("unexpected payload, expected a JSON array")
            return records
        except (OSError, ValueError) as exc:
            if attempt == retries:
                raise
            wait = backoff_s * 2 ** (attempt - 1)
            log.warning("Attempt %d/%d failed (%s), retrying in %.0fs", attempt, retries, exc, wait)
            time.sleep(wait)
    raise AssertionError("unreachable")


def landing_path(volume_root: str, run_ts: datetime) -> Path:
    """Hive-style ingest date folder, one file per run."""
    return (
        Path(volume_root)
        / "eco2mix"
        / f"ingest_date={run_ts.date().isoformat()}"
        / f"eco2mix_{run_ts.strftime('%Y%m%dT%H%M%SZ')}.jsonl"
    )


def run(volume_root: str, lookback_days: int = 2, today: date | None = None) -> Path | None:
    """Land the last `lookback_days` of data. Overlap is deduplicated in silver."""
    today = today or datetime.now(UTC).date()
    start = today - timedelta(days=lookback_days)
    records = fetch_records(build_export_url(start, today))
    if not records:
        log.info("No records between %s and %s", start, today)
        return None

    target = landing_path(volume_root, datetime.now(UTC))
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n")
    log.info("Landed %d records to %s", len(records), target)
    return target
