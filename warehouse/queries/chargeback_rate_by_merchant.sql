-- ============================================================
-- chargeback_rate_by_merchant.sql
--
-- Question:
--   Which merchants have the highest chargeback rate? Does it match
--   the risk_level they were assigned when onboarded?
--
-- Reads: fct_payments + dim_merchant
--
-- Tiny sample sizes are filtered out (captured_payments > 20) to avoid
-- noisy rates where 1 chargeback out of 3 payments looks like 33%.
-- ============================================================

WITH by_merchant AS (
    SELECT
        m.merchant_id,
        m.category,
        m.risk_level,
        COUNT(*)                                                AS total_payments,
        SUM(CASE WHEN p.status = 'captured' THEN 1 ELSE 0 END) AS captured_payments,
        SUM(CASE WHEN p.has_chargeback THEN 1 ELSE 0 END)      AS chargebacks,
        SUM(p.amount_usd)                                       AS gross_volume_usd
    FROM fct_payments p
    JOIN dim_merchant m USING (merchant_id)
    GROUP BY m.merchant_id, m.category, m.risk_level
)
SELECT
    merchant_id,
    category,
    risk_level,
    total_payments,
    captured_payments,
    chargebacks,
    ROUND(gross_volume_usd, 2)                                                    AS gross_volume_usd,
    ROUND(chargebacks::numeric / NULLIF(captured_payments, 0) * 100, 2)           AS chargeback_rate_pct
FROM by_merchant
WHERE captured_payments > 20
ORDER BY chargebacks::numeric / NULLIF(captured_payments, 0) DESC NULLS LAST
LIMIT 20;
