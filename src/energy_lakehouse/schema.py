"""Source-to-silver column mapping for the RTE eCO2mix dataset.

Source: ODRE open data, dataset `eco2mix-national-tr` (French national
electricity mix, 15-minute resolution, power values in MW).
"""

from __future__ import annotations

# source column -> silver column. All values are average power over the
# 15-minute interval, in MW. Negative values mean consumption by the asset
# (pumping, battery charging) or net imports for exchanges.
POWER_COLUMNS: dict[str, str] = {
    "consommation": "consumption_mw",
    "prevision_j1": "forecast_d1_mw",
    "prevision_j": "forecast_d0_mw",
    "nucleaire": "nuclear_mw",
    "eolien": "wind_mw",
    "eolien_terrestre": "wind_onshore_mw",
    "eolien_offshore": "wind_offshore_mw",
    "solaire": "solar_mw",
    "hydraulique": "hydro_mw",
    "hydraulique_fil_eau_eclusee": "hydro_run_of_river_mw",
    "hydraulique_lacs": "hydro_lakes_mw",
    "hydraulique_step_turbinage": "hydro_pumped_storage_generation_mw",
    "pompage": "hydro_pumping_mw",
    "bioenergies": "bioenergy_mw",
    "gaz": "gas_mw",
    "charbon": "coal_mw",
    "fioul": "oil_mw",
    "stockage_batterie": "battery_charge_mw",
    "destockage_batterie": "battery_discharge_mw",
    "ech_physiques": "net_exchanges_mw",
}

# Generation sources used for the production mix (sub-categories excluded so
# nothing is double counted).
GENERATION_SOURCES: list[str] = [
    "nuclear_mw",
    "wind_mw",
    "solar_mw",
    "hydro_mw",
    "bioenergy_mw",
    "gas_mw",
    "coal_mw",
    "oil_mw",
]

LOW_CARBON_SOURCES: list[str] = ["nuclear_mw", "wind_mw", "solar_mw", "hydro_mw", "bioenergy_mw"]

# 15-minute intervals: energy (MWh) = power (MW) * 0.25 h
INTERVAL_HOURS = 0.25
