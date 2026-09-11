"""Explicit Spark schemas for every raw dataset.

Bronze reads pin these schemas rather than inferring them, so the pipeline
fails loudly if the raw contract ever drifts. Bronze keeps timestamp-like
columns as StringType (raw representation preserved); silver casts them.
"""
from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("signup_date", StringType(), False),
    StructField("country", StringType(), False),
    StructField("marketing_channel", StringType(), False),
])

MERCHANTS_SCHEMA = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("risk_level", StringType(), False),
    StructField("created_at", StringType(), False),
])

PAYMENTS_SCHEMA = StructType([
    StructField("payment_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("status", StringType(), False),
    StructField("created_at", StringType(), False),
    StructField("updated_at", StringType(), False),
    StructField("method", StringType(), False),
])

CHARGEBACKS_SCHEMA = StructType([
    StructField("payment_id", StringType(), False),
    StructField("reason_code", StringType(), False),
    StructField("filed_at", StringType(), False),
])

FX_RATES_SCHEMA = StructType([
    StructField("date", StringType(), False),
    StructField("from_ccy", StringType(), False),
    StructField("to_ccy", StringType(), False),
    StructField("rate", DoubleType(), False),
])

EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("type", StringType(), False),
    StructField("event_ts", StringType(), False),
])

BLACKLIST_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("blacklist_updated_at", StringType(), False),
])
