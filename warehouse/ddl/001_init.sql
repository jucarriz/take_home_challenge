-- Aurelia DWH - gold layer schema
--
-- Executed by Postgres on first boot via /docker-entrypoint-initdb.d/.
-- Every statement is idempotent (CREATE ... IF NOT EXISTS) so the file
-- can also be re-applied by hand without wiping the database.
--
-- Star schema:
--   dim_customer, dim_merchant, dim_date  (dimensions)
--   fct_payments                          (grain: one row per payment)
--   agg_daily_kpis, agg_cohort_ltv        (pre-aggregated marts)
--
-- Spark writes to these tables via JDBC with mode=overwrite and
-- truncate=true, so the DDL below is the single source of truth for the
-- schema (Spark does not drop/recreate).

-- ---------------------------------------------------------------------------
-- Dimensions
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id        UUID          PRIMARY KEY,
    signup_date        DATE          NOT NULL,
    country            VARCHAR(2)    NOT NULL,
    marketing_channel  VARCHAR(50)   NOT NULL,
    cohort_month       DATE          NOT NULL,  -- first day of signup month
    is_blacklisted     BOOLEAN       NOT NULL DEFAULT FALSE,
    loaded_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_cohort  ON dim_customer(cohort_month);
CREATE INDEX IF NOT EXISTS idx_dim_customer_country ON dim_customer(country);

CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_id   VARCHAR(20)   PRIMARY KEY,
    category      VARCHAR(50)   NOT NULL,
    risk_level    VARCHAR(10)   NOT NULL CHECK (risk_level IN ('low','medium','high')),
    created_at    TIMESTAMPTZ   NOT NULL,
    loaded_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_merchant_category ON dim_merchant(category);
CREATE INDEX IF NOT EXISTS idx_dim_merchant_risk     ON dim_merchant(risk_level);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key       DATE         PRIMARY KEY,
    year           SMALLINT     NOT NULL,
    quarter        SMALLINT     NOT NULL,
    month          SMALLINT     NOT NULL,
    month_name     VARCHAR(10)  NOT NULL,
    day            SMALLINT     NOT NULL,
    day_of_week    SMALLINT     NOT NULL,  -- 0 = Monday, 6 = Sunday
    day_name       VARCHAR(10)  NOT NULL,
    week_of_year   SMALLINT     NOT NULL,
    is_weekend     BOOLEAN      NOT NULL
);

-- ---------------------------------------------------------------------------
-- Fact
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fct_payments (
    payment_id             VARCHAR(20)   PRIMARY KEY,
    customer_id            UUID          NOT NULL REFERENCES dim_customer(customer_id),
    merchant_id            VARCHAR(20)   NOT NULL REFERENCES dim_merchant(merchant_id),
    date_key               DATE          NOT NULL REFERENCES dim_date(date_key),
    amount_original        NUMERIC(18,4) NOT NULL CHECK (amount_original > 0),
    currency               VARCHAR(3)    NOT NULL,
    fx_rate_to_usd         NUMERIC(18,6) NOT NULL CHECK (fx_rate_to_usd > 0),
    amount_usd             NUMERIC(18,4) NOT NULL CHECK (amount_usd > 0),
    status                 VARCHAR(20)   NOT NULL CHECK (status IN ('captured','refused','refunded','pending')),
    method                 VARCHAR(20)   NOT NULL,
    created_at_utc         TIMESTAMPTZ   NOT NULL,
    updated_at_utc         TIMESTAMPTZ   NOT NULL,
    has_chargeback         BOOLEAN       NOT NULL DEFAULT FALSE,
    chargeback_reason_code VARCHAR(20),
    chargeback_filed_at    TIMESTAMPTZ,
    loaded_at              TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fct_payments_customer ON fct_payments(customer_id);
CREATE INDEX IF NOT EXISTS idx_fct_payments_merchant ON fct_payments(merchant_id);
CREATE INDEX IF NOT EXISTS idx_fct_payments_date     ON fct_payments(date_key);
CREATE INDEX IF NOT EXISTS idx_fct_payments_status   ON fct_payments(status);

-- ---------------------------------------------------------------------------
-- Aggregates (marts)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS agg_daily_kpis (
    date_key             DATE           PRIMARY KEY REFERENCES dim_date(date_key),
    total_payments       INTEGER        NOT NULL,
    captured_payments    INTEGER        NOT NULL,
    refused_payments     INTEGER        NOT NULL,
    refunded_payments    INTEGER        NOT NULL,
    auth_rate            NUMERIC(6,5)   NOT NULL CHECK (auth_rate BETWEEN 0 AND 1),
    chargeback_rate      NUMERIC(6,5)   NOT NULL CHECK (chargeback_rate BETWEEN 0 AND 1),
    gross_volume_usd     NUMERIC(18,4)  NOT NULL,
    captured_volume_usd  NUMERIC(18,4)  NOT NULL,
    unique_customers     INTEGER        NOT NULL,
    unique_merchants     INTEGER        NOT NULL,
    loaded_at            TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS agg_cohort_ltv (
    cohort_month         DATE           NOT NULL,
    months_since_signup  SMALLINT       NOT NULL,
    active_customers     INTEGER        NOT NULL,
    total_ltv_usd        NUMERIC(18,4)  NOT NULL,
    avg_ltv_usd          NUMERIC(18,4)  NOT NULL,
    loaded_at            TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cohort_month, months_since_signup)
);
