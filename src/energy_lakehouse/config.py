"""Table and volume names, derived from the Unity Catalog catalog of the target."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Paths:
    catalog: str

    @property
    def landing_volume(self) -> str:
        return f"/Volumes/{self.catalog}/bronze/landing"

    @property
    def landing_eco2mix(self) -> str:
        return f"{self.landing_volume}/eco2mix"

    def checkpoint(self, name: str) -> str:
        return f"/Volumes/{self.catalog}/bronze/checkpoints/{name}"

    @property
    def bronze_table(self) -> str:
        return f"{self.catalog}.bronze.eco2mix_raw"

    @property
    def silver_table(self) -> str:
        return f"{self.catalog}.silver.eco2mix"

    @property
    def quarantine_table(self) -> str:
        return f"{self.catalog}.silver.eco2mix_quarantine"

    @property
    def gold_daily_mix(self) -> str:
        return f"{self.catalog}.gold.daily_mix"

    @property
    def gold_daily_battery(self) -> str:
        return f"{self.catalog}.gold.daily_battery"
