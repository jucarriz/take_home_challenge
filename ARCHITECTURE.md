# Architecture

## Medallion overview

The pipeline follows the standard **medallion** pattern: three named
layers, each with a well-defined contract.

```mermaid
flowchart LR
    subgraph Sources
        RAW[data/raw/<br/>CSV + JSONL]
        API[Blacklist API<br/>FastAPI mock]
    end

    subgraph Bronze[Bronze: MinIO Parquet, partitioned by dt=YYYY-MM-DD]
        B_CUST[customers]
        B_MERCH[merchants]
        B_PAY[payments]
        B_CB[chargebacks]
        B_FX[fx_rates]
        B_EV[events]
        B_BL[blacklist]
    end

    subgraph Silver[Silver: MinIO Parquet, full snapshot]
        S_CUST[customers<br/>+ is_blacklisted<br/>+ cohort_month]
        S_PAY[payments<br/>+ amount_usd<br/>+ chargeback denormalized]
        S_OTHER[merchants / chargebacks<br/>fx_rates / events / blacklist]
    end

    subgraph Gold[Gold: Postgres star schema]
        DIM_C[dim_customer]
        DIM_M[dim_merchant]
        DIM_D[dim_date]
        FCT[fct_payments]
        AGG_D[agg_daily_kpis]
        AGG_C[agg_cohort_ltv]
    end

    subgraph Consumers
        Q[warehouse/queries/*.sql<br/>5 business queries]
        BI[BI tools / notebooks<br/>via JDBC]
    end

    RAW --> Bronze
    API --> B_BL
    Bronze --> Silver
    Silver --> Gold
    Gold --> Consumers
```

### Layer contracts

| Layer  | Storage             | Format         | Partition          | Contract                                                                 |
| ------ | ------------------- | -------------- | ------------------ | ------------------------------------------------------------------------ |
| Bronze | MinIO (S3)          | Parquet        | `dt=YYYY-MM-DD/`   | Raw data as ingested, no semantic changes. Adds `_ingested_at`.          |
| Silver | MinIO (S3)          | Parquet        | none (snapshot)    | Casts, UTC timestamps, FK integrity, FX priced in USD, denormalized CBs. |
| Gold   | Postgres 15         | Tables (star)  | none (small)       | Analytics-ready. FKs enforced. Aggregates pre-computed.                  |

## The DAGs

Two DAGs coexist. The batch one is the main flow; the streaming one
is an independent add-on demonstrating the event-driven pattern.

### `aurelia_daily` — batch

```mermaid
flowchart LR
    A[generate_data] --> B[ingest_bronze]
    B --> C[validate_bronze<br/>GE suite]
    C --> D[transform_silver]
    D --> E[validate_silver<br/>GE suite]
    E --> F[build_gold]
    F --> G[validate_gold<br/>GE suite]

    style C fill:#e8f4ff,stroke:#0066cc
    style E fill:#e8f4ff,stroke:#0066cc
    style G fill:#e8f4ff,stroke:#0066cc
```

- **Schedule**: `@daily`, `catchup=False`, `max_active_runs=1`.
- **Retries**: 1 per task, 1 min backoff, 30 min execution timeout.
- **Idempotency**: each task can be re-run safely — bronze pisa the
  `dt=` partition, silver is a full snapshot rewrite, gold does
  DELETE-reverse-FK + INSERT-forward-FK.

Each validate task is a Great Expectations suite that raises on
failure, breaking the chain and preventing downstream layers from
being materialized on bad data.

### `aurelia_streaming` — Kafka + Structured Streaming

```mermaid
flowchart LR
    P[produce_events<br/>kafka-python producer] --> C[consume_stream<br/>Spark Structured Streaming<br/>trigger=availableNow]

    RAW[(events.jsonl)] -.reads.-> P
    P -.publishes.-> TOPIC[[Kafka topic<br/>aurelia.events]]
    TOPIC -.subscribes.-> C
    C -.writes.-> BRONZE[(s3a://bronze/events_stream/)]

    style TOPIC fill:#fff4e6,stroke:#cc7700
```

- **Schedule**: `*/5 * * * *` (every 5 minutes), `catchup=False`,
  `max_active_runs=1`.
- **Producer** emits at 20 ms/message so the flow is visible in
  Kafka-UI (~40 s per micro-batch of 2000 events).
- **Consumer** uses `trigger=availableNow` and persists its offsets
  in a Spark checkpoint on a docker volume (not on S3, to avoid
  atomic-rename quirks of S3A with Structured Streaming).
- **Silver `_read_bronze_events_unioned`** joins the batch source
  (`bronze/events/dt=<ds>/`) with the streaming source
  (`bronze/events_stream/`) and dedupes on `event_id` — so if the
  streaming DAG never runs, silver still works from batch alone.

Evidence — the topic being fed by the producer, and the Parquet files
landed by the streaming consumer:

![Kafka topic filled by the producer](docs/screenshots/kafka_ui_topic.png)

![bronze/events_stream/ populated by Structured Streaming](docs/screenshots/minio_bronze_events_stream.png)

## Services (docker-compose)

```mermaid
flowchart TB
    subgraph Host[Host: Docker Compose network 'aurelia']
        direction TB

        subgraph Airflow[Airflow]
            AWEB[airflow-webserver<br/>:8080]
            ASCH[airflow-scheduler<br/>runs the DAG, hosts Spark]
            AINIT[airflow-init<br/>one-shot: db migrate + admin user]
        end

        PGA[(postgres-airflow<br/>:5433<br/>Airflow metadata)]
        PGD[(postgres-dwh<br/>:5432<br/>gold layer)]

        MINIO[(minio<br/>:9000 / :9001<br/>bronze + silver)]
        MINIT[minio-init<br/>one-shot: create buckets]

        API[blacklist-api<br/>:8000<br/>FastAPI mock]

        KAFKA[(kafka<br/>:9092 internal<br/>KRaft, no ZK)]
        KUI[kafka-ui<br/>:8081<br/>Provectus web UI]
    end

    AWEB --> PGA
    ASCH --> PGA
    AINIT --> PGA
    ASCH --> PGD
    ASCH --> MINIO
    ASCH --> API
    ASCH --> KAFKA
    KUI --> KAFKA
    MINIT --> MINIO
```

| Service            | Image                              | Role                                                    |
| ------------------ | ---------------------------------- | ------------------------------------------------------- |
| `postgres-dwh`     | `postgres:15-alpine`               | Gold warehouse. Loads `warehouse/ddl/*.sql` on first boot |
| `postgres-airflow` | `postgres:15-alpine`               | Airflow metadata (LocalExecutor requires it)              |
| `minio`            | `minio/minio`                      | S3-compatible object storage for bronze/silver            |
| `minio-init`       | `minio/mc`                         | Creates `bronze` / `silver` / `gold` buckets and exits    |
| `blacklist-api`    | `python:3.11-slim` + FastAPI       | Serves `/v1/blacklist` from `data/raw/blacklist.json`     |
| `airflow-init`     | `aurelia/airflow:local` (custom)   | Runs `airflow db migrate` and creates admin user          |
| `airflow-webserver`| `aurelia/airflow:local` (custom)   | UI on `:8080`                                             |
| `airflow-scheduler`| `aurelia/airflow:local` (custom)   | Runs the DAGs. Also hosts Spark in `local[*]` mode        |
| `kafka`            | `apache/kafka:3.7.2`               | Single-node broker in KRaft mode (no Zookeeper)           |
| `kafka-ui`         | `provectuslabs/kafka-ui`           | Web UI for the Kafka broker on `:8081`                    |

Custom Airflow image = official `apache/airflow:2.9.3-python3.11` +
Java (JRE for PySpark) + `pyspark`, `great-expectations`, `minio`,
`httpx`, `psycopg2-binary`, `pytest`, `fastapi`.

### Bucket lifecycle

The three MinIO buckets (`bronze`, `silver`, `gold`) are created on
first boot by the `minio-init` shell container in docker-compose so
the stack works with zero external tooling. The same buckets are also
declared in [`terraform/`](terraform/) using the `aminueza/minio`
provider — running `terraform apply` reconciles them idempotently.
Both paths coexist; pick whichever you prefer.

## Gold data model (star schema)

```mermaid
erDiagram
    dim_customer {
        UUID customer_id PK
        DATE signup_date
        VARCHAR country
        VARCHAR marketing_channel
        DATE cohort_month
        BOOLEAN is_blacklisted
    }
    dim_merchant {
        VARCHAR merchant_id PK
        VARCHAR category
        VARCHAR risk_level
        TIMESTAMPTZ created_at
    }
    dim_date {
        DATE date_key PK
        SMALLINT year
        SMALLINT quarter
        SMALLINT month
        VARCHAR month_name
        SMALLINT day
        SMALLINT day_of_week
        BOOLEAN is_weekend
    }
    fct_payments {
        VARCHAR payment_id PK
        UUID customer_id FK
        VARCHAR merchant_id FK
        DATE date_key FK
        NUMERIC amount_original
        VARCHAR currency
        NUMERIC fx_rate_to_usd
        NUMERIC amount_usd
        VARCHAR status
        VARCHAR method
        TIMESTAMPTZ created_at_utc
        TIMESTAMPTZ updated_at_utc
        BOOLEAN has_chargeback
        VARCHAR chargeback_reason_code
        TIMESTAMPTZ chargeback_filed_at
    }
    agg_daily_kpis {
        DATE date_key PK
        INTEGER total_payments
        INTEGER captured_payments
        NUMERIC auth_rate
        NUMERIC chargeback_rate
        NUMERIC gross_volume_usd
        NUMERIC captured_volume_usd
    }
    agg_cohort_ltv {
        DATE cohort_month PK
        SMALLINT months_since_signup PK
        INTEGER active_customers
        NUMERIC total_ltv_usd
        NUMERIC avg_ltv_usd
    }

    dim_customer ||--o{ fct_payments : has
    dim_merchant ||--o{ fct_payments : has
    dim_date     ||--o{ fct_payments : has
    dim_date     ||--o{ agg_daily_kpis : has
```

- **Grain of `fct_payments`**: one row per payment.
- **Grain of `agg_daily_kpis`**: one row per calendar day.
- **Grain of `agg_cohort_ltv`**: one row per `(cohort_month, months_since_signup)`.

Chargeback attributes are **denormalized** onto `fct_payments`
(`has_chargeback`, `chargeback_reason_code`, `chargeback_filed_at`) to
avoid joins in downstream queries. There is no `fct_chargebacks` fact
table on purpose.

## Data quality

Three Great Expectations suites, one per medallion layer, all
targeting the `payments` table (the fact of the whole platform).
Total: **26 expectations**.

| Suite               | Checks                                                                                              |
| ------------------- | --------------------------------------------------------------------------------------------------- |
| `bronze_payments`   | PK not null + unique, FK columns not null, `currency` / `status` / `method` in domain, `amount > 0` |
| `silver_payments`   | PK not null + unique, FKs not null, `created_at_utc` parsed, `amount_usd` / `fx_rate_to_usd` > 0    |
| `gold_fct_payments` | PK not null + unique, FKs not null, `date_key` not null, `status` in domain, `amount_usd > 0`       |

If any expectation fails, the corresponding `validate_*` task raises
and the downstream layers do not run — the pipeline stops on bad data.

## Idempotency and reproducibility

Both properties are enforced at every layer of the pipeline:

| Guarantee            | How it's achieved                                                                                                        |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Same input, same output | `random.seed(42)` in `generate_data.py`. UUIDs built from `random.getrandbits` (not `uuid.uuid4()`, which uses `os.urandom`). |
| Re-run of a partition   | Bronze writes to `dt=<ds>/` with `mode=overwrite` → the partition is pisada, siblings untouched.                          |
| Full refresh of gold    | `DELETE` in reverse-FK order + `INSERT` in forward-FK order. DDL, indexes and constraints untouched.                      |
| DDL applied once        | `warehouse/ddl/*.sql` mounted into `/docker-entrypoint-initdb.d/` — Postgres runs it on first boot only.                  |
| Env-driven config       | Every service reads its config from env vars with `${VAR:-default}` fallback, so the stack works without `.env`.          |
