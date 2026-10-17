-- Analysis queries on the gold layer (SQL editor or an AI/BI dashboard).
-- Replace energy_prod with the catalog of your target.

-- 1. Daily mix over the last 30 days
SELECT day,
       consumption_gwh,
       nuclear_gwh, wind_gwh, solar_gwh, hydro_gwh, gas_gwh,
       low_carbon_share,
       avg_carbon_intensity_g_per_kwh
FROM energy_prod.gold.daily_mix
WHERE day >= current_date() - INTERVAL 30 DAYS AND is_complete_day
ORDER BY day;

-- 2. Days where France exported the most electricity
SELECT day, net_exports_gwh, low_carbon_share
FROM energy_prod.gold.daily_mix
WHERE is_complete_day
ORDER BY net_exports_gwh DESC
LIMIT 10;

-- 3. Grid-scale battery activity trend (weekly)
SELECT date_trunc('WEEK', day) AS week,
       sum(charged_mwh)    AS charged_mwh,
       sum(discharged_mwh) AS discharged_mwh,
       max(peak_discharge_mw) AS peak_discharge_mw
FROM energy_prod.gold.daily_battery
GROUP BY 1
ORDER BY 1;

-- 4. What is failing data quality, and why
SELECT explode(_failed_rules) AS rule, count(*) AS rows
FROM energy_prod.silver.eco2mix_quarantine
WHERE _quarantined_at >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY 1
ORDER BY 2 DESC;
