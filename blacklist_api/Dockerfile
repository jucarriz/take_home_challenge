# Minimal image for the FastAPI mock of the external /v1/blacklist endpoint.
# Build context is ./blacklist_api (see docker-compose.yml).

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
# blacklist.json is added in Phase 2 (real endpoint implementation).

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
