"""Tests for scripts/produce_events.py — mock the Kafka broker."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import scripts.produce_events as pe


@pytest.fixture
def sample_events_file(tmp_path, monkeypatch):
    path = tmp_path / "events.jsonl"
    path.write_text(
        '{"event_id":"e1","customer_id":"c1","type":"login","event_ts":"2024-02-01T10:00:00"}\n'
        '{"event_id":"e2","customer_id":"c2","type":"failed_pin","event_ts":"2024-02-01T10:01:00"}\n'
        '{"event_id":"e3","customer_id":"c1","type":"logout","event_ts":"2024-02-01T10:02:00"}\n'
    )
    monkeypatch.setattr(pe, "EVENTS_PATH", path)
    return path


@patch("scripts.produce_events.KafkaProducer")
def test_produces_one_message_per_line(mock_kafka_producer, sample_events_file, monkeypatch):
    producer_mock = MagicMock()
    mock_kafka_producer.return_value = producer_mock

    monkeypatch.setattr(
        "sys.argv",
        ["produce_events.py", "--delay-ms", "0", "--bootstrap-servers", "test:9092"],
    )
    pe.main()

    assert producer_mock.send.call_count == 3


@patch("scripts.produce_events.KafkaProducer")
def test_message_key_is_customer_id(mock_kafka_producer, sample_events_file, monkeypatch):
    producer_mock = MagicMock()
    mock_kafka_producer.return_value = producer_mock

    monkeypatch.setattr(
        "sys.argv",
        ["produce_events.py", "--delay-ms", "0", "--bootstrap-servers", "test:9092"],
    )
    pe.main()

    keys = [call.kwargs["key"] for call in producer_mock.send.call_args_list]
    assert keys == ["c1", "c2", "c1"]


@patch("scripts.produce_events.KafkaProducer")
def test_limit_flag_caps_messages(mock_kafka_producer, sample_events_file, monkeypatch):
    producer_mock = MagicMock()
    mock_kafka_producer.return_value = producer_mock

    monkeypatch.setattr(
        "sys.argv",
        ["produce_events.py", "--delay-ms", "0", "--bootstrap-servers", "test:9092",
         "--limit", "2"],
    )
    pe.main()

    assert producer_mock.send.call_count == 2


def test_raises_when_events_file_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(pe, "EVENTS_PATH", tmp_path / "does-not-exist.jsonl")
    monkeypatch.setattr("sys.argv", ["produce_events.py"])
    with pytest.raises(FileNotFoundError):
        pe.main()
