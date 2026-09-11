"""Manually run the Aurelia pipeline end-to-end from the CLI.

Exists so the Spark jobs can be iterated on without going through the
Airflow DAG. The DAG (airflow/dags/aurelia_daily.py) runs the same steps
orchestrated with per-task Spark sessions and retries.

Usage from inside the Airflow scheduler container:

    docker exec -it aurelia-airflow-scheduler \\
        python /opt/airflow/scripts/run_pipeline.py --ds 2024-03-15

Skip stages that already ran:

    ... --skip-bronze --skip-silver

Prerequisites:
    * data/raw/ must be populated (run scripts/generate_data.py once).
    * MinIO + postgres-dwh + blacklist-api must be healthy.
"""
from __future__ import annotations

import argparse
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
LOG = logging.getLogger("run_pipeline")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ds", default="2024-03-15", help="Logical run date (YYYY-MM-DD)")
    parser.add_argument("--skip-bronze", action="store_true")
    parser.add_argument("--skip-silver", action="store_true")
    parser.add_argument("--skip-gold", action="store_true")
    args = parser.parse_args()

    from spark.common.spark_session import build_spark
    from spark.jobs import bronze_ingest, silver_clean, gold_marts

    spark = build_spark("run_pipeline")

    if not args.skip_bronze:
        LOG.info("=========== BRONZE ===========")
        bronze_ingest.ingest_customers(spark, args.ds)
        bronze_ingest.ingest_merchants(spark, args.ds)
        bronze_ingest.ingest_payments(spark, args.ds)
        bronze_ingest.ingest_chargebacks(spark, args.ds)
        bronze_ingest.ingest_fx_rates(spark, args.ds)
        bronze_ingest.ingest_events(spark, args.ds)
        bronze_ingest.ingest_blacklist(spark, args.ds)

    if not args.skip_silver:
        LOG.info("=========== SILVER ===========")
        silver_clean.transform_silver(spark, args.ds)

    if not args.skip_gold:
        LOG.info("=========== GOLD ===========")
        gold_marts.build_gold(spark)

    LOG.info("Pipeline finished for ds=%s", args.ds)


if __name__ == "__main__":
    main()
