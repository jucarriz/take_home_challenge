"""Publish the raw events dataset to Kafka topic ``aurelia.events``.

Reads ``data/raw/events.jsonl`` line by line and produces one message
per event. Uses ``customer_id`` as the message key so all events for a
customer land in the same partition (order preserved per customer).

To simulate real-time arrival, sleeps ``--delay-ms`` between messages
(default 20 ms → ~40 s to send the full 2000 events).

Usage from inside the Airflow scheduler container:

    docker exec -it aurelia-airflow-scheduler \\
        python /opt/airflow/scripts/produce_events.py --delay-ms 20
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path

from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
LOG = logging.getLogger("produce_events")

AURELIA_HOME = Path(os.getenv("AURELIA_HOME", "/opt/airflow"))
EVENTS_PATH = AURELIA_HOME / "data" / "raw" / "events.jsonl"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--delay-ms", type=int, default=20,
                        help="Delay between produced messages, milliseconds.")
    parser.add_argument("--bootstrap-servers",
                        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--topic",
                        default=os.getenv("KAFKA_TOPIC_EVENTS", "aurelia.events"))
    parser.add_argument("--limit", type=int, default=None,
                        help="Optional cap on number of messages (handy for testing).")
    args = parser.parse_args()

    if not EVENTS_PATH.exists():
        raise FileNotFoundError(
            f"{EVENTS_PATH} not found. Run scripts/generate_data.py first."
        )

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",
        linger_ms=5,
    )

    LOG.info(
        "Producing to %s @ %s (delay=%dms, limit=%s)",
        args.topic, args.bootstrap_servers, args.delay_ms, args.limit,
    )
    delay_seconds = args.delay_ms / 1000.0
    count = 0
    with EVENTS_PATH.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            producer.send(args.topic, key=event["customer_id"], value=line)
            count += 1
            if args.limit is not None and count >= args.limit:
                break
            if delay_seconds > 0:
                time.sleep(delay_seconds)

    producer.flush(timeout=30)
    producer.close(timeout=10)
    LOG.info("Produced %d events -> %s", count, args.topic)


if __name__ == "__main__":
    main()
