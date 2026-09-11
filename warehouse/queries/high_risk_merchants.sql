-- ============================================================
-- high_risk_merchants.sql
--
-- Question:
--   Which merchants concentrate the highest risk right now?
--   We combine three signals per merchant:
--     - declared risk_level = 'high'
--     - category is inherently risky (crypto, gaming)
--     - observed chargeback_rate > 2%
--
--   A merchant that trips more than one signal is a priority for
--   the risk team to review.
--
-- Reads: fct_payments + dim_merchant
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
),
scored AS (
    SELECT
        *,
        chargebacks::numeric / NULLIF(captured_payments, 0) AS chargeback_rate
    FROM by_merchant
)
SELECT
    merchant_id,
    category,
    risk_level,
    total_payments,
    chargebacks,
    ROUND(chargeback_rate * 100, 2)                       AS chargeback_rate_pct,
    ROUND(gross_volume_usd, 2)                            AS gross_volume_usd,
    (risk_level = 'high')                                 AS flag_risk_high,
    (category IN ('crypto', 'gaming'))                    AS flag_category_risky,
    (chargeback_rate > 0.02)                              AS flag_chargeback_over_2pct
FROM scored
WHERE risk_level = 'high'
   OR category IN ('crypto', 'gaming')
   OR chargeback_rate > 0.02
ORDER BY chargeback_rate DESC NULLS LAST, gross_volume_usd DESC;
