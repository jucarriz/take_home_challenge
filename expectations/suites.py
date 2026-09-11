"""Expectation suites for the payments table across the medallion layers.

Suites live as plain (method_name, kwargs) tuples so they're trivially
readable in a diff and easy to extend. The runner in :mod:`expectations.runner`
iterates the tuples and calls the corresponding ``validator.expect_*``
methods; each call both validates against the current DataFrame and
appends the expectation to the in-memory suite.

Payments is the fact table of the whole platform (every KPI derives
from it), so we focus checks on it at every layer:

    bronze_payments       - raw contract: schema domains + amount range.
    silver_payments       - post-clean: no orphans, USD priced, timestamps OK.
    gold_fct_payments     - final star schema: uniqueness + USD + domain.
"""
from __future__ import annotations

CURRENCIES = ["USD", "ARS", "BRL", "MXN", "EUR"]
STATUSES = ["captured", "refused", "refunded", "pending"]
METHODS = ["card", "wallet", "bank_transfer", "crypto"]
RISK_LEVELS = ["low", "medium", "high"]

BRONZE_PAYMENTS = [
    ("expect_column_values_to_not_be_null", {"column": "payment_id"}),
    ("expect_column_values_to_be_unique", {"column": "payment_id"}),
    ("expect_column_values_to_not_be_null", {"column": "customer_id"}),
    ("expect_column_values_to_not_be_null", {"column": "merchant_id"}),
    ("expect_column_values_to_be_in_set", {"column": "currency", "value_set": CURRENCIES}),
    ("expect_column_values_to_be_in_set", {"column": "status", "value_set": STATUSES}),
    ("expect_column_values_to_be_in_set", {"column": "method", "value_set": METHODS}),
    ("expect_column_values_to_be_between", {
        "column": "amount", "min_value": 0.01, "max_value": 100000,
    }),
]

SILVER_PAYMENTS = [
    ("expect_column_values_to_not_be_null", {"column": "payment_id"}),
    ("expect_column_values_to_be_unique", {"column": "payment_id"}),
    ("expect_column_values_to_not_be_null", {"column": "customer_id"}),
    ("expect_column_values_to_not_be_null", {"column": "merchant_id"}),
    ("expect_column_values_to_not_be_null", {"column": "created_at_utc"}),
    ("expect_column_values_to_not_be_null", {"column": "amount_usd"}),
    ("expect_column_values_to_be_in_set", {"column": "currency", "value_set": CURRENCIES}),
    ("expect_column_values_to_be_in_set", {"column": "status", "value_set": STATUSES}),
    ("expect_column_values_to_be_between", {
        "column": "amount_original", "min_value": 0.01,
    }),
    ("expect_column_values_to_be_between", {
        "column": "amount_usd", "min_value": 0.000001,
    }),
    ("expect_column_values_to_be_between", {
        "column": "fx_rate_to_usd", "min_value": 0.000001,
    }),
]

GOLD_FCT_PAYMENTS = [
    ("expect_column_values_to_not_be_null", {"column": "payment_id"}),
    ("expect_column_values_to_be_unique", {"column": "payment_id"}),
    ("expect_column_values_to_not_be_null", {"column": "customer_id"}),
    ("expect_column_values_to_not_be_null", {"column": "merchant_id"}),
    ("expect_column_values_to_not_be_null", {"column": "date_key"}),
    ("expect_column_values_to_be_in_set", {"column": "status", "value_set": STATUSES}),
    ("expect_column_values_to_be_between", {
        "column": "amount_usd", "min_value": 0.000001,
    }),
]

SUITES: dict[str, list[tuple[str, dict]]] = {
    "bronze_payments": BRONZE_PAYMENTS,
    "silver_payments": SILVER_PAYMENTS,
    "gold_fct_payments": GOLD_FCT_PAYMENTS,
}
