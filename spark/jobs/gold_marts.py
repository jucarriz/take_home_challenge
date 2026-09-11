"""Gold layer: build the star-schema tables in Postgres from silver.

The single entry point :func:`build_gold` runs everything in a coordinated
transaction-like sequence:

    1. DELETE every gold table in reverse-FK order (via psycopg2, since
       ``TRUNCATE`` would trip on the FKs).
    2. Insert dims, then the fact, then the aggregates.

Re-running produces the same rows (idempotent).
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common import io

LOG = logging.getLogger(__name__)


# ---- Dim / Fact / Agg builders --------------------------------------------

def _build_dim_customer(silver_customers: DataFrame) -> DataFrame:
    return silver_customers.select(
        "customer_id",
        "signup_date",
        "country",
        "marketing_channel",
        "cohort_month",
        "is_blacklisted",
    )


def _build_dim_merchant(silver_merchants: DataFrame) -> DataFrame:
    return silver_merchants.select(
        "merchant_id",
        "category",
        "risk_level",
        "created_at",
    )


def _build_dim_date(silver_payments: DataFrame) -> DataFrame:
    """Calendar table spanning [min(payment_date), max(payment_date)]."""
    bounds_row = silver_payments.agg(
        F.min(F.to_date("created_at_utc")).alias("min_date"),
        F.max(F.to_date("created_at_utc")).alias("max_date"),
    ).collect()[0]
    spark = silver_payments.sparkSession
    return (
        spark.sql(
            f"SELECT sequence(to_date('{bounds_row['min_date']}'), "
            f"to_date('{bounds_row['max_date']}'), interval 1 day) as _days"
        )
        .selectExpr("explode(_days) as date_key")
        .select(
            F.col("date_key"),
            F.year("date_key").cast("smallint").alias("year"),
            F.quarter("date_key").cast("smallint").alias("quarter"),
            F.month("date_key").cast("smallint").alias("month"),
            F.date_format("date_key", "MMMM").alias("month_name"),
            F.dayofmonth("date_key").cast("smallint").alias("day"),
            # Spark's dayofweek is 1=Sunday..7=Saturday. Remap to 0=Monday..6=Sunday.
            (((F.dayofweek("date_key") + 5) % 7)).cast("smallint").alias("day_of_week"),
            F.date_format("date_key", "EEEE").alias("day_name"),
            F.weekofyear("date_key").cast("smallint").alias("week_of_year"),
            F.dayofweek("date_key").isin(1, 7).alias("is_weekend"),
        )
    )


def _build_fct_payments(silver_payments: DataFrame) -> DataFrame:
    return silver_payments.select(
        "payment_id",
        "customer_id",
        "merchant_id",
        F.to_date("created_at_utc").alias("date_key"),
        "amount_original",
        "currency",
        "fx_rate_to_usd",
        "amount_usd",
        "status",
        "method",
        "created_at_utc",
        "updated_at_utc",
        "has_chargeback",
        "chargeback_reason_code",
        "chargeback_filed_at",
    )


def _build_agg_daily_kpis(fct_payments: DataFrame) -> DataFrame:
    grouped = (
        fct_payments.groupBy("date_key")
        .agg(
            F.count("*").alias("total_payments"),
            F.sum(F.when(F.col("status") == "captured", 1).otherwise(0)).alias("captured_payments"),
            F.sum(F.when(F.col("status") == "refused", 1).otherwise(0)).alias("refused_payments"),
            F.sum(F.when(F.col("status") == "refunded", 1).otherwise(0)).alias("refunded_payments"),
            F.sum("amount_usd").alias("gross_volume_usd"),
            F.sum(
                F.when(F.col("status") == "captured", F.col("amount_usd")).otherwise(0)
            ).alias("captured_volume_usd"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("merchant_id").alias("unique_merchants"),
            F.sum(F.when(F.col("has_chargeback"), 1).otherwise(0)).alias("chargeback_count"),
        )
    )
    return grouped.select(
        "date_key",
        "total_payments",
        "captured_payments",
        "refused_payments",
        "refunded_payments",
        (F.col("captured_payments") / F.col("total_payments"))
            .cast("decimal(6,5)").alias("auth_rate"),
        F.when(
            F.col("captured_payments") > 0,
            F.col("chargeback_count") / F.col("captured_payments"),
        ).otherwise(F.lit(0)).cast("decimal(6,5)").alias("chargeback_rate"),
        F.col("gross_volume_usd").cast("decimal(18,4)").alias("gross_volume_usd"),
        F.col("captured_volume_usd").cast("decimal(18,4)").alias("captured_volume_usd"),
        "unique_customers",
        "unique_merchants",
    )


def _build_agg_cohort_ltv(dim_customer: DataFrame, fct_payments: DataFrame) -> DataFrame:
    captured = fct_payments.filter(F.col("status") == "captured")
    joined = (
        captured.join(dim_customer.select("customer_id", "cohort_month"), "customer_id")
        .withColumn(
            "months_since_signup",
            (
                (F.year("date_key") - F.year("cohort_month")) * 12
                + (F.month("date_key") - F.month("cohort_month"))
            ).cast("smallint"),
        )
    )
    grouped = joined.groupBy("cohort_month", "months_since_signup").agg(
        F.countDistinct("customer_id").alias("active_customers"),
        F.sum("amount_usd").alias("total_ltv_usd"),
    )
    return grouped.select(
        "cohort_month",
        "months_since_signup",
        "active_customers",
        F.col("total_ltv_usd").cast("decimal(18,4)").alias("total_ltv_usd"),
        (F.col("total_ltv_usd") / F.col("active_customers"))
            .cast("decimal(18,4)").alias("avg_ltv_usd"),
    )


# ---- Entry point ----------------------------------------------------------

def build_gold(spark: SparkSession) -> None:
    silver_customers = io.read_silver(spark, "customers")
    silver_merchants = io.read_silver(spark, "merchants")
    silver_payments = io.read_silver(spark, "payments")

    dim_customer = _build_dim_customer(silver_customers)
    dim_merchant = _build_dim_merchant(silver_merchants)
    dim_date = _build_dim_date(silver_payments)
    fct_payments = _build_fct_payments(silver_payments)
    agg_daily_kpis = _build_agg_daily_kpis(fct_payments)
    agg_cohort_ltv = _build_agg_cohort_ltv(dim_customer, fct_payments)

    # Cache the fact so the two aggregates don't re-scan silver.
    fct_payments = fct_payments.cache()

    LOG.info("Wiping existing gold rows (reverse-FK order)")
    io.truncate_gold_tables()

    LOG.info("Loading dims")
    io.write_gold(dim_customer, "dim_customer")
    io.write_gold(dim_merchant, "dim_merchant")
    io.write_gold(dim_date, "dim_date")

    LOG.info("Loading fct_payments")
    io.write_gold(fct_payments, "fct_payments")

    LOG.info("Loading aggregates")
    io.write_gold(agg_daily_kpis, "agg_daily_kpis")
    io.write_gold(agg_cohort_ltv, "agg_cohort_ltv")

    fct_payments.unpersist()
    LOG.info("gold refresh complete")
