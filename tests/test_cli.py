import pytest

from energy_lakehouse.cli import parse_args
from energy_lakehouse.config import Paths


def test_parse_args():
    args = parse_args(["silver", "--catalog", "energy_dev"])
    assert args.step == "silver"
    assert args.catalog == "energy_dev"
    assert args.lookback_days == 2


def test_unknown_step_is_rejected():
    with pytest.raises(SystemExit):
        parse_args(["platinum", "--catalog", "x"])


def test_paths_are_scoped_to_catalog():
    paths = Paths("energy_prod")
    assert paths.bronze_table == "energy_prod.bronze.eco2mix_raw"
    assert paths.landing_eco2mix == "/Volumes/energy_prod/bronze/landing/eco2mix"
    assert paths.checkpoint("silver") == "/Volumes/energy_prod/bronze/checkpoints/silver"
