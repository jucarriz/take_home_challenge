"""Aurelia daily pipeline: raw -> bronze -> silver -> gold.

Runs every day, but is trivially retriggerable from the Airflow UI. Every
task is idempotent for a given ``{{ ds }}``:

    * ``generate_data`` skips regeneration if all raw files already exist.
    * ``ingest_bronze`` writes to ``s3a://bronze/<table>/dt=<ds>/`` with
      ``mode=overwrite`` -> pisa la particion sin tocar hermanas.
    * ``transform_silver`` overwrites the whole silver snapshot.
    * ``build_gold`` DELETEs (reverse-FK) and INSERTs the gold tables.

Design note (single bronze task vs 7 parallel):
    Splitting bronze into per-table parallel PythonOperator tasks was
    considered because it would show nicer parallelism in the DAG graph.
    In practice each task boots its own Spark JVM in ``local[*]`` mode
    (~1-2 GB RSS each), so 7 parallel tasks would OOM a laptop-sized
    container. Ingesting the 7 datasets sequentially inside a single
    SparkSession is <15 s total, so the trade-off is not worth it. If
    the pipeline ever moved to a real Spark cluster, splitting into
    parallel tasks with a proper resource pool would become correct.
"""
from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

REPO_ROOT = Path("/opt/airflow")

DEFAULT_ARGS = {
    "owner": "aurelia",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="aurelia_daily",
    description="Medallion refresh: raw -> bronze -> silver -> gold.",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["aurelia", "medallion"],
    doc_md=__doc__,
)
def aurelia_daily():

    @task(doc_md="Runs `scripts/generate_data.py`. Idempotent: skips if files exist.")
    def generate_data() -> None:
        subprocess.run(
            [sys.executable, str(REPO_ROOT / "scripts" / "generate_data.py")],
            check=True,
        )

    @task(doc_md="Land the 7 raw datasets in MinIO under `s3a://bronze/<t>/dt=<ds>/`.")
    def ingest_bronze() -> None:
        from spark.common.spark_session import build_spark
        from spark.jobs import bronze_ingest

        ds = get_current_context()["ds"]
        spark = build_spark("aurelia-bronze")
        try:
            bronze_ingest.ingest_customers(spark, ds)
            bronze_ingest.ingest_merchants(spark, ds)
            bronze_ingest.ingest_payments(spark, ds)
            bronze_ingest.ingest_chargebacks(spark, ds)
            bronze_ingest.ingest_fx_rates(spark, ds)
            bronze_ingest.ingest_events(spark, ds)
            bronze_ingest.ingest_blacklist(spark, ds)
        finally:
            spark.stop()

    @task(doc_md="Cast to UTC, enforce FKs, price in USD, denormalize chargebacks.")
    def transform_silver() -> None:
        from spark.common.spark_session import build_spark
        from spark.jobs import silver_clean

        ds = get_current_context()["ds"]
        spark = build_spark("aurelia-silver")
        try:
            silver_clean.transform_silver(spark, ds)
        finally:
            spark.stop()

    @task(doc_md="Rebuild dims + fct + aggs in Postgres from silver.")
    def build_gold() -> None:
        from spark.common.spark_session import build_spark
        from spark.jobs import gold_marts

        spark = build_spark("aurelia-gold")
        try:
            gold_marts.build_gold(spark)
        finally:
            spark.stop()

    generate_data() >> ingest_bronze() >> transform_silver() >> build_gold()


aurelia_daily()
