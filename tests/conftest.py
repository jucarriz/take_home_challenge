"""Pytest fixtures shared across the whole test suite."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """A quiet, single-threaded SparkSession reused for every test.

    ``local[1]`` guarantees deterministic ordering when tests
    ``.collect()`` DataFrames, and ``shuffle.partitions=1`` skips the
    default 200-partition shuffle overhead on tiny fixtures.
    """
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("aurelia-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()
