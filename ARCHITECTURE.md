# Architecture

> This document is a stub. It will be completed at the end of the
> implementation with the final diagrams and the actual services deployed.

## Medallion architecture

- **Bronze** — raw data as ingested from source, stored as Parquet in
  MinIO under `s3a://bronze/<table>/dt=YYYY-MM-DD/`. No semantic changes.
- **Silver** — cleaned and standardized. Type casting, UTC timestamps,
  foreign-key validation, FX conversion to USD, blacklist enrichment.
- **Gold** — analytics-ready star schema (facts + dimensions +
  pre-aggregated marts) stored in PostgreSQL.

## Services

| Service           | Role                                                   |
| ----------------- | ------------------------------------------------------ |
| postgres-dwh      | Gold layer warehouse                                    |
| postgres-airflow  | Airflow metadata database                               |
| minio             | S3-compatible object storage for bronze/silver         |
| airflow-webserver | Airflow UI                                              |
| airflow-scheduler | Runs the DAG                                            |
| blacklist-api     | FastAPI mock of the external `/v1/blacklist` endpoint  |

## DAG

```
generate_data_if_missing
    → ingest_bronze_* (parallel)
    → validate_bronze
    → transform_silver
    → validate_silver
    → build_gold
    → validate_gold
```

Runs daily, `catchup=False`, idempotent per `{{ ds }}` partition.

## Data model (gold)

Star schema:

- `dim_customer`, `dim_merchant`, `dim_date`
- `fct_payments`
- `agg_daily_kpis`, `agg_cohort_ltv`

DDL lives in [warehouse/ddl/001_init.sql](warehouse/ddl/001_init.sql) and
is executed by Postgres on first boot via `docker-entrypoint-initdb.d/`.
