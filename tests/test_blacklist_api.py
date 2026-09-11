"""Tests for the FastAPI blacklist mock."""
from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

import blacklist_api.main as api_module


@pytest.fixture
def blacklist_file(tmp_path, monkeypatch):
    """Write a small blacklist.json to a temp dir and point the API at it."""
    path = tmp_path / "blacklist.json"
    path.write_text(json.dumps({
        "blacklisted_customer_ids": ["c1", "c2"],
        "updated_at": "2024-03-31T23:59:59+00:00",
    }))
    monkeypatch.setattr(api_module, "BLACKLIST_PATH", path)
    return path


@pytest.fixture
def missing_blacklist(tmp_path, monkeypatch):
    monkeypatch.setattr(api_module, "BLACKLIST_PATH", tmp_path / "nope.json")


def test_healthz_returns_ok(blacklist_file):
    client = TestClient(api_module.app)
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_blacklist_returns_json_payload(blacklist_file):
    client = TestClient(api_module.app)
    response = client.get("/v1/blacklist")
    assert response.status_code == 200
    body = response.json()
    assert body["blacklisted_customer_ids"] == ["c1", "c2"]
    assert body["updated_at"] == "2024-03-31T23:59:59+00:00"


def test_blacklist_returns_503_when_file_missing(missing_blacklist):
    client = TestClient(api_module.app)
    response = client.get("/v1/blacklist")
    assert response.status_code == 503
    assert "not found" in response.json()["detail"].lower()
