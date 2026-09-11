"""Structured Streaming: consume Kafka topic ``aurelia.events`` and land
messages as Parquet under ``s3a://bronze/events_stream/``.

Uses ``trigger=availableNow`` so each Airflow micro-batch processes
whatever messages are currently in the topic and exits. The Spark
checkpoint (persisted on a docker volume, not S3) guarantees each
message is written exactly once across runs.
"""
from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

LOG = logging.getLogger(__name__)

EVENT_JSON_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("type", StringType(), False),
    StructField("event_ts", StringType(), False),
])


def stream_events(spark: SparkSession) -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC_EVENTS", "aurelia.events")
    checkpoint_dir = os.getenv(
        "KAFKA_CHECKPOINT_DIR",
        "/opt/airflow/checkpoints/events_stream",
    )
    bucket = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
    output_path = f"s3a://{bucket}/events_stream/"

    LOG.info(
        "Streaming %s @ %s -> %s (checkpoint=%s)",
        topic, bootstrap, output_path, checkpoint_dir,
    )

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
        .select(
            F.from_json("json_str", EVENT_JSON_SCHEMA).alias("event"),
            F.col("kafka_ts"),
        )
        .select("event.*", "kafka_ts")
    )

    query = (
        parsed.writeStream
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    LOG.info("Streaming micro-batch complete")
