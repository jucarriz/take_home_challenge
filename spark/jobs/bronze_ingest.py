"""Bronze layer: read raw datasets and land them in MinIO as Parquet.

Every function writes to ``s3a://bronze/<table>/dt=<ds>/`` so re-running
for the same ``ds`` overwrites that single partition (idempotent) without
touching sibling partitions.

Bronze preserves the raw representation - no casts, no filters. Only a
``_ingested_at`` timestamp is added so the layer stays traceable.
"""
from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from spark.common import io, schemas

LOG = logging.getLogger(__name__)


def _ingest_csv(spark: SparkSession, ds: str, table: str, filename: str, schema) -> None:
    df = io.read_csv_raw(spark, filename, schema)
    path = io.write_bronze(df, table, ds)
    LOG.info("bronze %s -> %s (%d rows)", table, path, df.count())


def ingest_customers(spark: SparkSession, ds: str) -> None:
    _ingest_csv(spark, ds, "customers", "customers.csv", schemas.CUSTOMERS_SCHEMA)


def ingest_merchants(spark: SparkSession, ds: str) -> None:
    _ingest_csv(spark, ds, "merchants", "merchants.csv", schemas.MERCHANTS_SCHEMA)


def ingest_payments(spark: SparkSession, ds: str) -> None:
    _ingest_csv(spark, ds, "payments", "payments.csv", schemas.PAYMENTS_SCHEMA)


def ingest_chargebacks(spark: SparkSession, ds: str) -> None:
    _ingest_csv(spark, ds, "chargebacks", "chargebacks.csv", schemas.CHARGEBACKS_SCHEMA)


def ingest_fx_rates(spark: SparkSession, ds: str) -> None:
    _ingest_csv(spark, ds, "fx_rates", "fx_rates.csv", schemas.FX_RATES_SCHEMA)


def ingest_events(spark: SparkSession, ds: str) -> None:
    df = io.read_jsonl_raw(spark, "events.jsonl", schemas.EVENTS_SCHEMA)
    path = io.write_bronze(df, "events", ds)
    LOG.info("bronze events -> %s (%d rows)", path, df.count())


def ingest_blacklist(spark: SparkSession, ds: str) -> None:
    """The blacklist is served by the FastAPI mock, not by a file."""
    rows = io.fetch_blacklist_rows()
    df = spark.createDataFrame(rows, schemas.BLACKLIST_SCHEMA)
    path = io.write_bronze(df, "blacklist", ds)
    LOG.info("bronze blacklist -> %s (%d rows)", path, df.count())
