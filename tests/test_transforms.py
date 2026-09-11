"""Unit tests for the silver + gold Spark transformations."""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.jobs.gold_marts import _build_agg_daily_kpis
from spark.jobs.silver_clean import (
    _clean_customers,
    _enrich_customers,
    _enrich_payments,
)


# ----- silver: cleaners ---------------------------------------------------

def test_clean_customers_casts_signup_date(spark):
    df = spark.createDataFrame(
        [("c1", "2024-01-15", "AR", "facebook")],
        ["customer_id", "signup_date", "country", "marketing_channel"],
    )
    row = _clean_customers(df).first()
    assert row.signup_date == date(2024, 1, 15)


# ----- silver: enrichers --------------------------------------------------

def test_enrich_customers_adds_cohort_and_blacklist(spark):
    customers = spark.createDataFrame(
        [
            ("c1", date(2024, 1, 15), "AR", "facebook"),
            ("c2", date(2024, 2, 10), "US", "google"),
        ],
        StructType([
            StructField("customer_id", StringType()),
            StructField("signup_date", DateType()),
            StructField("country", StringType()),
            StructField("marketing_channel", StringType()),
        ]),
    )
    blacklist = spark.createDataFrame([("c1",)], ["customer_id"])

    by_id = {r.customer_id: r for r in _enrich_customers(customers, blacklist).collect()}
    assert by_id["c1"].is_blacklisted is True
    assert by_id["c1"].cohort_month == date(2024, 1, 1)
    assert by_id["c2"].is_blacklisted is False
    assert by_id["c2"].cohort_month == date(2024, 2, 1)


PAYMENTS_SCHEMA = StructType([
    StructField("payment_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("amount_original", DecimalType(18, 4)),
    StructField("currency", StringType()),
    StructField("status", StringType()),
    StructField("method", StringType()),
    StructField("created_at_utc", TimestampType()),
    StructField("updated_at_utc", TimestampType()),
])

FX_SCHEMA = StructType([
    StructField("date", DateType()),
    StructField("from_ccy", StringType()),
    StructField("to_ccy", StringType()),
    StructField("rate", DecimalType(18, 6)),
])

CUSTOMERS_MIN_SCHEMA = StructType([
    StructField("customer_id", StringType()),
    StructField("signup_date", DateType()),
])

CHARGEBACKS_SCHEMA = StructType([
    StructField("payment_id", StringType()),
    StructField("reason_code", StringType()),
    StructField("filed_at", TimestampType()),
])


def _payment_row(pid, cid, mid, amount, currency, status, ts):
    return (pid, cid, mid, Decimal(str(amount)), currency, status, "card", ts, ts)


def test_enrich_payments_drops_orphan_customer(spark):
    customers = spark.createDataFrame([], CUSTOMERS_MIN_SCHEMA)
    merchants = spark.createDataFrame([("m1",)], ["merchant_id"])
    fx = spark.createDataFrame([], FX_SCHEMA)
    chargebacks = spark.createDataFrame([], CHARGEBACKS_SCHEMA)
    payments = spark.createDataFrame(
        [_payment_row("p1", "c_ghost", "m1", 100, "USD", "captured",
                      datetime(2024, 2, 1, tzinfo=timezone.utc))],
        PAYMENTS_SCHEMA,
    )
    assert _enrich_payments(payments, customers, merchants, fx, chargebacks).count() == 0


def test_enrich_payments_drops_pre_signup(spark):
    customers = spark.createDataFrame(
        [("c1", date(2024, 3, 1))], CUSTOMERS_MIN_SCHEMA,
    )
    merchants = spark.createDataFrame([("m1",)], ["merchant_id"])
    fx = spark.createDataFrame(
        [(date(2024, 2, 1), "USD", "USD", Decimal("1.0"))], FX_SCHEMA,
    )
    chargebacks = spark.createDataFrame([], CHARGEBACKS_SCHEMA)
    payments = spark.createDataFrame(
        # Payment dated Feb but customer signed up in March -> drop.
        [_payment_row("p1", "c1", "m1", 100, "USD", "captured",
                      datetime(2024, 2, 1, tzinfo=timezone.utc))],
        PAYMENTS_SCHEMA,
    )
    assert _enrich_payments(payments, customers, merchants, fx, chargebacks).count() == 0


def test_enrich_payments_converts_ars_to_usd(spark):
    customers = spark.createDataFrame(
        [("c1", date(2024, 1, 1))], CUSTOMERS_MIN_SCHEMA,
    )
    merchants = spark.createDataFrame([("m1",)], ["merchant_id"])
    fx = spark.createDataFrame(
        [(date(2024, 2, 1), "ARS", "USD", Decimal("0.0012"))], FX_SCHEMA,
    )
    chargebacks = spark.createDataFrame([], CHARGEBACKS_SCHEMA)
    payments = spark.createDataFrame(
        [_payment_row("p1", "c1", "m1", 1000, "ARS", "captured",
                      datetime(2024, 2, 1, tzinfo=timezone.utc))],
        PAYMENTS_SCHEMA,
    )
    row = _enrich_payments(payments, customers, merchants, fx, chargebacks).first()
    assert float(row.amount_usd) == pytest.approx(1.2, rel=1e-4)
    assert row.has_chargeback is False


# ----- gold: aggregation --------------------------------------------------

FCT_MIN_SCHEMA = StructType([
    StructField("date_key", DateType()),
    StructField("status", StringType()),
    StructField("amount_usd", DecimalType(18, 4)),
    StructField("customer_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("has_chargeback", BooleanType()),
])


def test_agg_daily_kpis_auth_rate_and_chargeback_rate(spark):
    d = date(2024, 2, 1)
    rows = (
        [(d, "captured", Decimal("100.0"), "c1", "m1", True) for _ in range(2)]
        + [(d, "captured", Decimal("100.0"), "c1", "m1", False) for _ in range(6)]
        + [(d, "refused",  Decimal("50.0"),  "c1", "m1", False)]
        + [(d, "refunded", Decimal("60.0"),  "c1", "m1", False)]
    )
    fct = spark.createDataFrame(rows, FCT_MIN_SCHEMA)

    result = _build_agg_daily_kpis(fct).first()
    assert result.date_key == d
    assert result.total_payments == 10
    assert result.captured_payments == 8
    assert result.refused_payments == 1
    assert result.refunded_payments == 1
    # 8 captured / 10 total = 0.8
    assert float(result.auth_rate) == pytest.approx(0.8, rel=1e-4)
    # 2 chargebacks / 8 captured = 0.25
    assert float(result.chargeback_rate) == pytest.approx(0.25, rel=1e-4)
