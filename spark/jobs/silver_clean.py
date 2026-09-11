"""Silver layer: cast raw types, standardize timestamps to UTC, apply FK
integrity, join FX rates to compute ``amount_usd``, join the blacklist,
and denormalize chargebacks onto payments.

Reads the bronze partition of the given ``ds`` and writes a full silver
snapshot (unpartitioned) since gold is always a full recompute.
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common import io

LOG = logging.getLogger(__name__)


# ---- Per-table cleanups (types + names) -----------------------------------

def _clean_customers(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("customer_id"),
        F.to_date("signup_date", "yyyy-MM-dd").alias("signup_date"),
        F.col("country"),
        F.col("marketing_channel"),
    )


def _clean_merchants(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("merchant_id"),
        F.col("category"),
        F.col("risk_level"),
        F.to_timestamp("created_at").alias("created_at"),
    )


def _clean_payments(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("payment_id"),
        F.col("customer_id"),
        F.col("merchant_id"),
        F.col("amount").cast("decimal(18,4)").alias("amount_original"),
        F.col("currency"),
        F.col("status"),
        F.col("method"),
        F.to_timestamp("created_at").alias("created_at_utc"),
        F.to_timestamp("updated_at").alias("updated_at_utc"),
    )


def _clean_chargebacks(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("payment_id"),
        F.col("reason_code"),
        F.to_timestamp("filed_at").alias("filed_at"),
    )


def _clean_fx_rates(df: DataFrame) -> DataFrame:
    return df.select(
        F.to_date("date", "yyyy-MM-dd").alias("date"),
        F.col("from_ccy"),
        F.col("to_ccy"),
        F.col("rate").cast("decimal(18,6)").alias("rate"),
    )


def _clean_events(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("event_id"),
        F.col("customer_id"),
        F.col("type"),
        F.to_timestamp("event_ts").alias("event_ts"),
    )


# ---- Enrichment (joins) ---------------------------------------------------

def _enrich_customers(customers: DataFrame, blacklist: DataFrame) -> DataFrame:
    """Add is_blacklisted and cohort_month to customers."""
    return (
        customers
        .join(blacklist.withColumn("_bl_hit", F.lit(True)), "customer_id", "left")
        .withColumn("is_blacklisted", F.col("_bl_hit").isNotNull())
        .withColumn("cohort_month", F.trunc("signup_date", "MM"))
        .drop("_bl_hit")
    )


def _enrich_payments(
    payments: DataFrame,
    customers: DataFrame,
    merchants: DataFrame,
    fx_rates: DataFrame,
    chargebacks: DataFrame,
) -> DataFrame:
    """Enforce FKs, business-time rule, price in USD, denormalize chargebacks."""
    customers_ref = customers.select("customer_id", "signup_date")
    valid_merchant_ids = merchants.select("merchant_id")
    payments_joined = (
        payments
        .join(customers_ref, "customer_id", "inner")
        .join(valid_merchant_ids, "merchant_id", "inner")
    )

    # Business rule: a payment cannot precede its customer's signup date.
    # The seed enforces this too, so in a healthy run this drops 0 rows;
    # keeping the guard here as defense-in-depth against upstream drift.
    invalid_time = payments_joined.filter(
        F.to_date("created_at_utc") < F.col("signup_date")
    ).count()
    if invalid_time > 0:
        LOG.warning(
            "Dropping %d payments dated before their customer signup_date",
            invalid_time,
        )
    payments_valid = (
        payments_joined
        .filter(F.to_date("created_at_utc") >= F.col("signup_date"))
        .drop("signup_date")
    )

    fx = fx_rates.selectExpr(
        "date as fx_date", "from_ccy as fx_ccy", "rate as fx_rate"
    )
    payments_with_fx = (
        payments_valid
        .withColumn("_payment_date", F.to_date("created_at_utc"))
        .join(
            fx,
            (F.col("_payment_date") == F.col("fx_date"))
            & (F.col("currency") == F.col("fx_ccy")),
            "left",
        )
    )

    missing_fx = payments_with_fx.filter(F.col("fx_rate").isNull()).count()
    if missing_fx > 0:
        LOG.warning("Dropping %d payments with no FX rate for their date/currency", missing_fx)

    payments_priced = (
        payments_with_fx
        .filter(F.col("fx_rate").isNotNull())
        .withColumn("fx_rate_to_usd", F.col("fx_rate"))
        .withColumn(
            "amount_usd",
            (F.col("amount_original") * F.col("fx_rate")).cast("decimal(18,4)"),
        )
        .drop("fx_date", "fx_ccy", "fx_rate", "_payment_date")
    )

    return (
        payments_priced
        .join(chargebacks, "payment_id", "left")
        .withColumn("has_chargeback", F.col("reason_code").isNotNull())
        .withColumnRenamed("reason_code", "chargeback_reason_code")
        .withColumnRenamed("filed_at", "chargeback_filed_at")
    )


# ---- Entry point ----------------------------------------------------------

def transform_silver(spark: SparkSession, ds: str) -> None:
    bronze_customers = io.read_bronze(spark, "customers", ds)
    bronze_merchants = io.read_bronze(spark, "merchants", ds)
    bronze_payments = io.read_bronze(spark, "payments", ds)
    bronze_chargebacks = io.read_bronze(spark, "chargebacks", ds)
    bronze_fx = io.read_bronze(spark, "fx_rates", ds)
    bronze_events = io.read_bronze(spark, "events", ds)
    bronze_blacklist = io.read_bronze(spark, "blacklist", ds)

    customers = _clean_customers(bronze_customers)
    merchants = _clean_merchants(bronze_merchants)
    payments = _clean_payments(bronze_payments)
    chargebacks = _clean_chargebacks(bronze_chargebacks)
    fx_rates = _clean_fx_rates(bronze_fx)
    events = _clean_events(bronze_events)
    blacklist = bronze_blacklist.select("customer_id").distinct()

    customers_final = _enrich_customers(customers, blacklist)
    payments_final = _enrich_payments(
        payments, customers, merchants, fx_rates, chargebacks
    )

    io.write_silver(customers_final, "customers")
    io.write_silver(merchants, "merchants")
    io.write_silver(payments_final, "payments")
    io.write_silver(chargebacks, "chargebacks")
    io.write_silver(fx_rates, "fx_rates")
    io.write_silver(events, "events")
    io.write_silver(blacklist, "blacklist")

    LOG.info("silver refresh done for ds=%s", ds)
