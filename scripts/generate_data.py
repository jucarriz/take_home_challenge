"""Deterministic seed for the Aurelia data platform.

Generates the raw datasets required by the pipeline under `data/raw/`
using a fixed random seed. Re-running produces byte-identical output,
which is a hard requirement for idempotent bronze ingestion.

Usage
-----
    python scripts/generate_data.py            # skip if all files exist
    python scripts/generate_data.py --force    # regenerate

Only Python stdlib. No pip install required.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

SEED = 42

# Business timeframe for the simulated data.
START_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)
END_DATE = datetime(2024, 3, 31, 23, 59, 59, tzinfo=timezone.utc)

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"

# Volume knobs — kept modest so the whole pipeline runs in seconds on a laptop
# while producing enough rows for the KPIs to be meaningful.
N_CUSTOMERS = 500
N_MERCHANTS = 50
N_PAYMENTS = 5000
CHARGEBACK_FRACTION_OF_CAPTURED = 0.04
N_EVENTS = 2000
N_BLACKLIST = 10
N_SUSPICIOUS_CUSTOMERS = 10  # accounts skewed toward failed_pin events

COUNTRIES = ["AR", "US", "BR", "MX", "CL", "ES"]
COUNTRY_WEIGHTS = [0.35, 0.25, 0.15, 0.10, 0.08, 0.07]

MARKETING_CHANNELS = ["facebook", "google", "organic", "tiktok", "referral"]
MARKETING_WEIGHTS = [0.30, 0.25, 0.20, 0.15, 0.10]

MERCHANT_CATEGORIES = [
    "retail", "groceries", "crypto", "gaming", "travel", "saas", "digital_content"
]

CURRENCIES = ["USD", "ARS", "BRL", "MXN", "EUR"]
CURRENCY_WEIGHTS = [0.30, 0.35, 0.15, 0.10, 0.10]

PAYMENT_STATUSES = ["captured", "refused", "refunded", "pending"]
PAYMENT_STATUS_WEIGHTS_DEFAULT = [0.82, 0.12, 0.04, 0.02]
PAYMENT_STATUS_WEIGHTS_HIGH_RISK = [0.65, 0.25, 0.05, 0.05]

PAYMENT_METHODS = ["card", "wallet", "bank_transfer", "crypto"]
METHOD_WEIGHTS = [0.55, 0.25, 0.15, 0.05]

EVENT_TYPES = ["login", "failed_pin", "password_reset", "kyc_submit"]
EVENT_TYPE_WEIGHTS = [0.60, 0.15, 0.15, 0.10]

CHARGEBACK_REASONS = ["FRAUD", "DISPUTED", "NOT_RECEIVED", "DUPLICATE"]

# Approximate real-world FX rates to USD as of early 2024.
FX_BASE_TO_USD = {
    "USD": 1.0,
    "ARS": 0.0012,
    "BRL": 0.20,
    "MXN": 0.058,
    "EUR": 1.08,
}


def _rand_datetime_between(start: datetime, end: datetime) -> datetime:
    delta_seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, delta_seconds))


def _risk_for_category(category: str) -> str:
    if category in {"crypto", "gaming"}:
        return random.choices(["high", "medium", "low"], weights=[0.6, 0.3, 0.1])[0]
    if category in {"travel", "digital_content"}:
        return random.choices(["medium", "low", "high"], weights=[0.5, 0.4, 0.1])[0]
    return random.choices(["low", "medium", "high"], weights=[0.7, 0.25, 0.05])[0]


def _rand_uuid4() -> str:
    """Deterministic UUID v4 seeded from the module-level random stream.

    ``uuid.uuid4()`` uses ``os.urandom`` and would break reproducibility,
    so we build the UUID ourselves from ``random.getrandbits``.
    """
    return str(uuid.UUID(int=random.getrandbits(128), version=4))


def gen_customers() -> list[dict]:
    out = []
    for _ in range(N_CUSTOMERS):
        out.append({
            "customer_id": _rand_uuid4(),
            "signup_date": _rand_datetime_between(START_DATE, END_DATE).date().isoformat(),
            "country": random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0],
            "marketing_channel": random.choices(MARKETING_CHANNELS, weights=MARKETING_WEIGHTS)[0],
        })
    return out


def gen_merchants() -> list[dict]:
    out = []
    for i in range(N_MERCHANTS):
        category = random.choice(MERCHANT_CATEGORIES)
        out.append({
            "merchant_id": f"m_{i + 1:04d}",
            "category": category,
            "risk_level": _risk_for_category(category),
            "created_at": _rand_datetime_between(START_DATE, END_DATE).isoformat(),
        })
    return out


def gen_payments(customers: list[dict], merchants: list[dict]) -> list[dict]:
    merchants_by_id = {m["merchant_id"]: m for m in merchants}
    out = []
    for i in range(N_PAYMENTS):
        customer = random.choice(customers)
        merchant = random.choice(merchants)
        # A payment cannot happen before the customer exists. Anchor the
        # lower bound on the customer's signup_date (business rule that
        # silver_clean enforces defensively too).
        signup_dt = datetime.fromisoformat(customer["signup_date"]).replace(tzinfo=timezone.utc)
        created_at = _rand_datetime_between(max(signup_dt, START_DATE), END_DATE)
        updated_at = created_at + timedelta(minutes=random.randint(0, 720))
        # Log-normal so most payments are small, with a long tail of big ones.
        raw_amount = random.lognormvariate(mu=3.5, sigma=1.2)
        amount = round(max(1.0, min(raw_amount, 50000.0)), 2)
        weights = (
            PAYMENT_STATUS_WEIGHTS_HIGH_RISK
            if merchants_by_id[merchant["merchant_id"]]["risk_level"] == "high"
            else PAYMENT_STATUS_WEIGHTS_DEFAULT
        )
        out.append({
            "payment_id": f"p_{i + 1:06d}",
            "customer_id": customer["customer_id"],
            "merchant_id": merchant["merchant_id"],
            "amount": amount,
            "currency": random.choices(CURRENCIES, weights=CURRENCY_WEIGHTS)[0],
            "status": random.choices(PAYMENT_STATUSES, weights=weights)[0],
            "created_at": created_at.isoformat(),
            "updated_at": updated_at.isoformat(),
            "method": random.choices(PAYMENT_METHODS, weights=METHOD_WEIGHTS)[0],
        })
    return out


def gen_chargebacks(payments: list[dict]) -> list[dict]:
    captured = [p for p in payments if p["status"] == "captured"]
    n = int(len(captured) * CHARGEBACK_FRACTION_OF_CAPTURED)
    sampled = random.sample(captured, n)
    out = []
    for p in sampled:
        payment_dt = datetime.fromisoformat(p["created_at"])
        filed_at = payment_dt + timedelta(days=random.randint(5, 30))
        out.append({
            "payment_id": p["payment_id"],
            "reason_code": random.choice(CHARGEBACK_REASONS),
            "filed_at": filed_at.isoformat(),
        })
    return out


def gen_fx_rates() -> list[dict]:
    out = []
    day: date = START_DATE.date()
    end_day: date = END_DATE.date()
    while day <= end_day:
        for ccy, base in FX_BASE_TO_USD.items():
            noise = random.uniform(-0.05, 0.05)
            rate = round(base * (1 + noise), 6)
            out.append({
                "date": day.isoformat(),
                "from_ccy": ccy,
                "to_ccy": "USD",
                "rate": rate,
            })
        day += timedelta(days=1)
    return out


def gen_events(customers: list[dict]) -> list[dict]:
    suspicious = {c["customer_id"] for c in random.sample(customers, N_SUSPICIOUS_CUSTOMERS)}
    out = []
    for i in range(N_EVENTS):
        customer = random.choice(customers)
        if customer["customer_id"] in suspicious and random.random() < 0.5:
            event_type = "failed_pin"
        else:
            event_type = random.choices(EVENT_TYPES, weights=EVENT_TYPE_WEIGHTS)[0]
        out.append({
            "event_id": f"e_{i + 1:06d}",
            "customer_id": customer["customer_id"],
            "type": event_type,
            "event_ts": _rand_datetime_between(START_DATE, END_DATE).isoformat(),
        })
    return out


def gen_blacklist(customers: list[dict]) -> dict:
    return {
        "blacklisted_customer_ids": sorted(
            c["customer_id"] for c in random.sample(customers, N_BLACKLIST)
        ),
        "updated_at": END_DATE.isoformat(),  # deterministic (not now())
    }


def _write_csv(path: Path, rows: Iterable[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_jsonl(path: Path, rows: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)


OUTPUT_FILES = [
    RAW_DIR / "customers.csv",
    RAW_DIR / "merchants.csv",
    RAW_DIR / "payments.csv",
    RAW_DIR / "chargebacks.csv",
    RAW_DIR / "fx_rates.csv",
    RAW_DIR / "events.jsonl",
    RAW_DIR / "blacklist.json",
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate every file even if it already exists.",
    )
    args = parser.parse_args()

    if not args.force and all(p.exists() for p in OUTPUT_FILES):
        print("All raw files already exist; nothing to do (use --force to regenerate).")
        return

    random.seed(SEED)

    customers = gen_customers()
    merchants = gen_merchants()
    payments = gen_payments(customers, merchants)
    chargebacks = gen_chargebacks(payments)
    fx_rates = gen_fx_rates()
    events = gen_events(customers)
    blacklist = gen_blacklist(customers)

    _write_csv(
        RAW_DIR / "customers.csv", customers,
        ["customer_id", "signup_date", "country", "marketing_channel"],
    )
    _write_csv(
        RAW_DIR / "merchants.csv", merchants,
        ["merchant_id", "category", "risk_level", "created_at"],
    )
    _write_csv(
        RAW_DIR / "payments.csv", payments,
        ["payment_id", "customer_id", "merchant_id", "amount", "currency",
         "status", "created_at", "updated_at", "method"],
    )
    _write_csv(
        RAW_DIR / "chargebacks.csv", chargebacks,
        ["payment_id", "reason_code", "filed_at"],
    )
    _write_csv(
        RAW_DIR / "fx_rates.csv", fx_rates,
        ["date", "from_ccy", "to_ccy", "rate"],
    )
    _write_jsonl(RAW_DIR / "events.jsonl", events)
    _write_json(RAW_DIR / "blacklist.json", blacklist)

    print(
        f"Generated: {len(customers)} customers, {len(merchants)} merchants, "
        f"{len(payments)} payments, {len(chargebacks)} chargebacks, "
        f"{len(fx_rates)} fx rates, {len(events)} events, "
        f"{len(blacklist['blacklisted_customer_ids'])} blacklisted customers "
        f"-> {RAW_DIR}"
    )


if __name__ == "__main__":
    main()
