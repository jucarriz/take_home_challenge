"""Tests for scripts/generate_data.py."""
from __future__ import annotations

import random
import sys
from datetime import date

import pytest

from scripts import generate_data as gd


def _run_generators() -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """Run the generator functions in the same order main() does."""
    random.seed(gd.SEED)
    customers = gd.gen_customers()
    merchants = gd.gen_merchants()
    payments = gd.gen_payments(customers, merchants)
    chargebacks = gd.gen_chargebacks(payments)
    return customers, merchants, payments, chargebacks


def test_customer_rowcount():
    customers, *_ = _run_generators()
    assert len(customers) == gd.N_CUSTOMERS


def test_merchants_rowcount():
    _, merchants, _, _ = _run_generators()
    assert len(merchants) == gd.N_MERCHANTS


def test_payments_rowcount():
    _, _, payments, _ = _run_generators()
    assert len(payments) == gd.N_PAYMENTS


def test_chargebacks_are_a_subset_of_captured_payments():
    _, _, payments, chargebacks = _run_generators()
    captured_ids = {p["payment_id"] for p in payments if p["status"] == "captured"}
    for cb in chargebacks:
        assert cb["payment_id"] in captured_ids


def test_no_payment_predates_signup():
    """Business rule regression: gen_payments must anchor created_at on
    the customer signup_date."""
    customers, _, payments, _ = _run_generators()
    signup_by_id = {c["customer_id"]: date.fromisoformat(c["signup_date"]) for c in customers}
    violations = [
        p for p in payments
        if date.fromisoformat(p["created_at"][:10]) < signup_by_id[p["customer_id"]]
    ]
    assert violations == [], f"{len(violations)} payments predate their customer signup"


def test_seed_is_deterministic(tmp_path, monkeypatch):
    """Running main() twice with --force produces identical bytes."""
    new_outputs = [tmp_path / p.name for p in gd.OUTPUT_FILES]
    monkeypatch.setattr(gd, "RAW_DIR", tmp_path)
    monkeypatch.setattr(gd, "OUTPUT_FILES", new_outputs)
    monkeypatch.setattr(sys, "argv", ["generate_data.py", "--force"])

    gd.main()
    first = {p.name: p.read_bytes() for p in tmp_path.iterdir()}

    gd.main()
    second = {p.name: p.read_bytes() for p in tmp_path.iterdir()}

    assert first == second


def test_skip_when_all_files_exist(tmp_path, monkeypatch, capsys):
    """Without --force, if every output file exists, main() is a no-op."""
    new_outputs = [tmp_path / p.name for p in gd.OUTPUT_FILES]
    monkeypatch.setattr(gd, "RAW_DIR", tmp_path)
    monkeypatch.setattr(gd, "OUTPUT_FILES", new_outputs)
    for p in new_outputs:
        p.write_bytes(b"placeholder")
    monkeypatch.setattr(sys, "argv", ["generate_data.py"])
    gd.main()
    out = capsys.readouterr().out
    assert "already exist" in out
