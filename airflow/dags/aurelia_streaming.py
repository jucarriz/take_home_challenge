"""Aurelia streaming pipeline: Kafka producer + Spark Structured Streaming
consumer for the events dataset.

Runs every 5 minutes as an independent DAG (parallel to ``aurelia_daily``).
Two tasks:

    produce_events    reads data/raw/events.jsonl and publishes to the
                      Kafka topic ``aurelia.events`` with a 20 ms
                      delay per message so the flow is visible in
                      Kafka-UI (localhost:8081).

    consume_stream    Structured Streaming micro-batch
                      (``trigger=availableNow``) that reads the topic
                      from the last committed offset (checkpoint) and
                      writes new messages as Parquet to
                      ``s3a://bronze/events_stream/``.

The batch DAG ``aurelia_daily`` continues to ingest the same events
from the file into ``s3a://bronze/events/dt=<ds>/``. Silver later
does ``UNION`` + ``dropDuplicates(event_id)`` of both sources, so the
two paths coexist without conflict.
"""
from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

REPO_ROOT = Path("/opt/airflow")

DEFAULT_ARGS = {
    "owner": "aurelia",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=15),
}


@dag(
    dag_id="aurelia_streaming",
    description="Kafka producer + Spark Structured Streaming consumer for events.",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["aurelia", "streaming"],
    doc_md=__doc__,
)
def aurelia_streaming():

    @task(doc_md="Publish events.jsonl to Kafka topic `aurelia.events` at 20 ms/msg.")
    def produce_events() -> None:
        subprocess.run(
            [
                sys.executable,
                str(REPO_ROOT / "scripts" / "produce_events.py"),
                "--delay-ms", "20",
            ],
            check=True,
        )

    @task(doc_md="Consume new messages from Kafka and write Parquet to bronze/events_stream/.")
    def consume_stream() -> None:
        from spark.common.spark_session import build_spark
        from spark.jobs.stream_events import stream_events

        spark = build_spark("aurelia-stream")
        try:
            stream_events(spark)
        finally:
            spark.stop()

    produce_events() >> consume_stream()


aurelia_streaming()
