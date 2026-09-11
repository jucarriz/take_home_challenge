"""IO helpers for reading raw data, writing to MinIO (bronze/silver) and
writing to Postgres (gold).

All paths and connection details are resolved from environment variables
with sensible defaults for the docker-compose stack, so the same code
runs unchanged inside Airflow and in local pytest fixtures (just set
``AURELIA_HOME`` to the repo root and point MinIO/Postgres vars at
your test containers).
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

import httpx
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

AURELIA_HOME = Path(os.getenv("AURELIA_HOME", "/opt/airflow"))
RAW_DIR = AURELIA_HOME / "data" / "raw"

BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")


# ---- Path builders --------------------------------------------------------

def _raw_uri(filename: str) -> str:
    return f"file://{(RAW_DIR / filename).as_posix()}"


def bronze_path(table: str, ds: str) -> str:
    return f"s3a://{BUCKET_BRONZE}/{table}/dt={ds}/"


def silver_path(table: str) -> str:
    return f"s3a://{BUCKET_SILVER}/{table}/"


# ---- Raw readers ----------------------------------------------------------

def read_csv_raw(spark: SparkSession, filename: str, schema) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(_raw_uri(filename))
    )


def read_jsonl_raw(spark: SparkSession, filename: str, schema) -> DataFrame:
    return spark.read.schema(schema).json(_raw_uri(filename))


def fetch_blacklist_rows() -> list[dict]:
    """Hit the blacklist mock API and return one row per blacklisted customer."""
    url = os.getenv("BLACKLIST_API_URL", "http://blacklist-api:8000/v1/blacklist")
    response = httpx.get(url, timeout=10.0)
    response.raise_for_status()
    payload = response.json()
    return [
        {"customer_id": cid, "blacklist_updated_at": payload["updated_at"]}
        for cid in payload["blacklisted_customer_ids"]
    ]


# ---- Bronze / Silver (MinIO) ---------------------------------------------

def write_bronze(df: DataFrame, table: str, ds: str) -> str:
    path = bronze_path(table, ds)
    (
        df.withColumn("_ingested_at", F.current_timestamp())
        .write.mode("overwrite")
        .parquet(path)
    )
    return path


def read_bronze(spark: SparkSession, table: str, ds: str) -> DataFrame:
    return spark.read.parquet(bronze_path(table, ds))


def write_silver(df: DataFrame, table: str) -> str:
    path = silver_path(table)
    df.write.mode("overwrite").parquet(path)
    return path


def read_silver(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.parquet(silver_path(table))


# ---- Gold (Postgres) ------------------------------------------------------

def _jdbc_url() -> str:
    host = os.getenv("POSTGRES_DWH_HOST", "postgres-dwh")
    port = os.getenv("POSTGRES_DWH_PORT", "5432")
    db = os.getenv("POSTGRES_DWH_DB", "aurelia_dwh")
    # stringtype=unspecified lets Postgres infer the column type from the
    # target schema. Without it, Spark sends every string as `character
    # varying` and Postgres refuses to cast into UUID / DATE / TIMESTAMPTZ
    # columns (strict typing). This is the standard workaround for
    # Spark-JDBC-to-Postgres when the target has non-string types like UUID.
    return f"jdbc:postgresql://{host}:{port}/{db}?stringtype=unspecified"


def _jdbc_user() -> str:
    return os.getenv("POSTGRES_DWH_USER", "aurelia")


def _jdbc_password() -> str:
    return os.getenv("POSTGRES_DWH_PASSWORD", "aurelia")


def read_gold_table(spark: SparkSession, table: str) -> DataFrame:
    """Read a gold table from Postgres via JDBC.

    Useful for Great Expectations checks against the materialized gold
    layer (bronze/silver checks read Parquet directly).
    """
    return (
        spark.read
        .format("jdbc")
        .option("url", _jdbc_url())
        .option("dbtable", table)
        .option("user", _jdbc_user())
        .option("password", _jdbc_password())
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def write_gold(df: DataFrame, table: str) -> None:
    """Insert into a gold table via JDBC (append mode).

    We deliberately do NOT use ``truncate=true`` here because the FK
    constraints between gold tables would make TRUNCATE fail. Callers must
    invoke :func:`truncate_gold_tables` first if they want a full refresh.
    """
    (
        df.write
        .format("jdbc")
        .option("url", _jdbc_url())
        .option("dbtable", table)
        .option("user", _jdbc_user())
        .option("password", _jdbc_password())
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


# Tables in the order they must be DELETEd so the FKs don't complain.
GOLD_TABLES_DELETE_ORDER: list[str] = [
    "agg_cohort_ltv",
    "agg_daily_kpis",
    "fct_payments",
    "dim_customer",
    "dim_merchant",
    "dim_date",
]


def truncate_gold_tables(tables: Iterable[str] | None = None) -> None:
    """DELETE FROM every gold table in reverse-FK order.

    We can't use ``TRUNCATE`` because of the FK constraints between
    fct_payments/aggs and the dims. DELETE keeps the DDL untouched
    (indexes, checks, PKs) and lets Spark then append cleanly.
    """
    import psycopg2  # lazy so unit tests can mock

    tables_list = list(tables) if tables is not None else GOLD_TABLES_DELETE_ORDER
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_DWH_HOST", "postgres-dwh"),
        port=int(os.getenv("POSTGRES_DWH_PORT", "5432")),
        user=_jdbc_user(),
        password=_jdbc_password(),
        dbname=os.getenv("POSTGRES_DWH_DB", "aurelia_dwh"),
    )
    try:
        with conn:
            with conn.cursor() as cur:
                for table in tables_list:
                    cur.execute(f"DELETE FROM {table}")
    finally:
        conn.close()
