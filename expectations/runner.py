"""Run a suite of Great Expectations against a Spark DataFrame.

Uses an EphemeralDataContext so nothing hits disk (no ``great_expectations.yml``,
no data-docs site, no store backend). Perfect for pipeline-time
validation — the check either passes or raises :class:`SuiteFailed`,
which surfaces as a failed Airflow task and stops the DAG.

Design note: we use the "validator" pattern where each ``validator.expect_*``
call both records the expectation into the in-memory suite AND runs it
immediately, so we can log a per-expectation verdict without needing a
separate Checkpoint.
"""
from __future__ import annotations

import logging
import uuid

import great_expectations as gx
from pyspark.sql import DataFrame

from expectations.suites import SUITES

LOG = logging.getLogger(__name__)


class SuiteFailed(RuntimeError):
    """Raised when one or more expectations in a suite failed."""


def validate(df: DataFrame, suite_name: str) -> None:
    """Validate ``df`` against the named suite. Raise on any failure.

    Parameters
    ----------
    df:
        Spark DataFrame to validate.
    suite_name:
        Key into :data:`expectations.suites.SUITES`.
    """
    if suite_name not in SUITES:
        raise KeyError(f"Unknown expectation suite: {suite_name}")

    context = gx.get_context(mode="ephemeral")

    # Unique names per call so the same context can be reused across suites
    # without name clashes on datasource / asset.
    tag = uuid.uuid4().hex[:8]
    datasource = context.sources.add_spark(name=f"ds_{suite_name}_{tag}")
    asset = datasource.add_dataframe_asset(name=f"asset_{suite_name}_{tag}")
    batch_request = asset.build_batch_request(dataframe=df)

    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name=f"suite_{suite_name}_{tag}",
    )

    expectations = SUITES[suite_name]
    failed: list[tuple[str, dict, object]] = []
    for method_name, kwargs in expectations:
        result = getattr(validator, method_name)(**kwargs)
        if not result.success:
            failed.append((method_name, kwargs, result))
            LOG.error(
                "FAIL %s(%s) - unexpected_count=%s",
                method_name,
                kwargs,
                getattr(result, "result", {}).get("unexpected_count"),
            )

    passed = len(expectations) - len(failed)
    LOG.info("Suite %s: %d/%d expectations passed", suite_name, passed, len(expectations))

    if failed:
        raise SuiteFailed(
            f"Suite {suite_name}: {len(failed)}/{len(expectations)} expectations failed"
        )
