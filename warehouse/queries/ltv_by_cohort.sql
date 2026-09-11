-- ============================================================
-- ltv_by_cohort.sql
--
-- Question:
--   How does average LTV (in USD) evolve month by month for each
--   signup cohort? Which cohort retains and monetizes better?
--
-- Reads: agg_cohort_ltv + dim_customer
--
-- Output shape: long format (one row per cohort x months_since_signup).
-- Retention is computed against the size of the cohort at month 0.
-- ============================================================

WITH cohort_size AS (
    SELECT cohort_month, COUNT(*) AS cohort_customers
    FROM dim_customer
    GROUP BY cohort_month
)
SELECT
    a.cohort_month,
    cs.cohort_customers,
    a.months_since_signup,
    a.active_customers,
    ROUND(a.active_customers::numeric / cs.cohort_customers * 100, 2)  AS retention_pct,
    ROUND(a.total_ltv_usd, 2)                                          AS total_ltv_usd,
    ROUND(a.avg_ltv_usd, 2)                                            AS avg_ltv_usd
FROM agg_cohort_ltv a
JOIN cohort_size cs USING (cohort_month)
ORDER BY a.cohort_month, a.months_since_signup;
