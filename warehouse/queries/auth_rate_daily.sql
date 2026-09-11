-- ============================================================
-- auth_rate_daily.sql
--
-- Question:
--   How is the daily payment authorization rate evolving? Are there
--   days with abnormal dips that warrant investigation?
--
-- Reads: agg_daily_kpis
--
-- Run from PowerShell / bash on the host:
--   Get-Content warehouse/queries/auth_rate_daily.sql | `
--       docker exec -i aurelia-postgres-dwh psql -U aurelia -d aurelia_dwh
-- ============================================================

SELECT
    date_key,
    total_payments,
    captured_payments,
    ROUND(auth_rate * 100, 2)                 AS auth_rate_pct,
    ROUND(chargeback_rate * 100, 2)           AS chargeback_rate_pct,
    ROUND(gross_volume_usd, 2)                AS gross_volume_usd,
    ROUND(captured_volume_usd, 2)             AS captured_volume_usd,
    unique_customers,
    unique_merchants
FROM agg_daily_kpis
ORDER BY date_key;
