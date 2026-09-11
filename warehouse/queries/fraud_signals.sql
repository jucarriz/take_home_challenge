-- ============================================================
-- fraud_signals.sql
--
-- Question:
--   Which customers show multiple fraud-adjacent signals and should
--   be prioritized for manual review?
--
--   Three orthogonal signals per customer:
--     1. blacklist          -> flagged by the external /v1/blacklist API
--     2. multi_chargeback   -> >= 2 chargebacks in the period
--     3. risky_merchants    -> > 30% of the volume went to merchants
--                              tagged high risk or in crypto/gaming
--
--   Customers with >=2 signals are the priority queue.
--
-- Reads: dim_customer + dim_merchant + fct_payments
--
-- Future work: enriquecer con la tabla fct_events (login / failed_pin /
-- password_reset) para agregar una senal de "authentication anomalies".
-- Requiere materializar events en gold, hoy solo esta en silver.
-- ============================================================

WITH cust_stats AS (
    SELECT
        p.customer_id,
        COUNT(*)                                                AS total_payments,
        SUM(CASE WHEN p.has_chargeback THEN 1 ELSE 0 END)      AS chargebacks,
        SUM(p.amount_usd)                                       AS total_volume_usd,
        SUM(CASE
                WHEN m.risk_level = 'high' OR m.category IN ('crypto', 'gaming')
                THEN p.amount_usd
                ELSE 0
            END)                                                AS risky_volume_usd
    FROM fct_payments p
    JOIN dim_merchant m USING (merchant_id)
    GROUP BY p.customer_id
),
flagged AS (
    SELECT
        c.customer_id,
        c.country,
        c.cohort_month,
        c.is_blacklisted,
        s.total_payments,
        s.chargebacks,
        ROUND(s.total_volume_usd, 2)                                          AS total_volume_usd,
        ROUND(s.risky_volume_usd / NULLIF(s.total_volume_usd, 0) * 100, 2)    AS risky_volume_pct,
        c.is_blacklisted                                                       AS signal_blacklist,
        (s.chargebacks >= 2)                                                   AS signal_multi_chargeback,
        (s.risky_volume_usd / NULLIF(s.total_volume_usd, 0) > 0.30)            AS signal_risky_merchants
    FROM cust_stats s
    JOIN dim_customer c USING (customer_id)
)
SELECT
    customer_id,
    country,
    cohort_month,
    total_payments,
    chargebacks,
    total_volume_usd,
    risky_volume_pct,
    signal_blacklist,
    signal_multi_chargeback,
    signal_risky_merchants,
    (signal_blacklist::int + signal_multi_chargeback::int + signal_risky_merchants::int)
                                                                              AS signals_triggered
FROM flagged
WHERE signal_blacklist
   OR signal_multi_chargeback
   OR signal_risky_merchants
ORDER BY signals_triggered DESC, total_volume_usd DESC
LIMIT 50;
