"""Mock of the external /v1/blacklist API consumed by the Aurelia pipeline.

Phase 1: only /healthz is implemented so the container has a working
healthcheck and the rest of the stack can depend on it.
Phase 2 adds GET /v1/blacklist backed by blacklist.json.
"""
from fastapi import FastAPI

app = FastAPI(title="Aurelia Blacklist Mock", version="0.1.0")


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}
