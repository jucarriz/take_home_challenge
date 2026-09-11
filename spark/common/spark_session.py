"""SparkSession builder shared by every Aurelia Spark job.

Pins Java packages needed to talk to MinIO (hadoop-aws) and Postgres
(JDBC). These are downloaded to ~/.ivy2 on the first Spark job and
cached afterwards, so the first pipeline run takes ~1 min longer than
subsequent runs.

Session time zone is fixed to UTC so ``to_timestamp`` casts across
inputs with mixed tz suffixes are unambiguous.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

HADOOP_AWS_VERSION = "3.3.4"
AWS_SDK_VERSION = "1.12.262"
POSTGRES_JDBC_VERSION = "42.7.3"
SPARK_KAFKA_VERSION = "3.5.1"  # matches PySpark 3.5.1 / Scala 2.12

_PACKAGES = ",".join([
    f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
    f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_VERSION}",
    f"org.postgresql:postgresql:{POSTGRES_JDBC_VERSION}",
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_KAFKA_VERSION}",
])


def build_spark(app_name: str = "aurelia") -> SparkSession:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", _PACKAGES)
        .config("spark.sql.session.timeZone", "UTC")
        # S3A -> MinIO
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Sensible defaults for a single-node local run
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
    )
    return builder.getOrCreate()
