# Plan definitivo — Aurelia Data Platform

**Stack final:** Python + PySpark + Apache Airflow + Great Expectations + PostgreSQL + MinIO + Docker Compose + pytest + FastAPI (solo para el mock `/v1/blacklist`).

**Decisiones cerradas:**
- FastAPI no se fuerza: solo se usa para el mock de blacklist que consume el pipeline.
- Procesamiento con PySpark (no dbt).
- Sin Alembic/ORM migrations: schema gold definido en `warehouse/ddl/init.sql` cargado por Postgres vía `docker-entrypoint-initdb.d/`.
- Seed = script `scripts/generate_data.py` que produce los CSVs/JSONL en `/data/raw/`.
- Kafka y Terraform quedan como *future work* en el README.
- Docker Desktop se instalará más adelante (aviso antes de la Fase 1).

**Tiempo objetivo:** 8–10 horas (según pide el PDF).

---

## Estructura final del repo

```
aurelia-data-platform/
├── README.md
├── ARCHITECTURE.md
├── docker-compose.yml
├── .env.example
├── .gitignore
├── pyproject.toml            # deps con uv o poetry (o requirements.txt)
│
├── data/
│   └── raw/                  # generado por scripts/generate_data.py
│       ├── customers.csv
│       ├── merchants.csv
│       ├── payments.csv
│       ├── chargebacks.csv
│       ├── fx_rates.csv
│       ├── events.jsonl
│       └── blacklist.json
│
├── scripts/
│   ├── generate_data.py      # seed
│   └── bootstrap_minio.py    # crea buckets bronze/silver/gold al arrancar
│
├── warehouse/
│   ├── ddl/
│   │   └── 001_init.sql      # CREATE TABLE IF NOT EXISTS de tablas gold
│   └── queries/
│       ├── auth_rate_daily.sql
│       ├── chargeback_rate.sql
│       ├── ltv_by_cohort.sql
│       ├── high_risk_merchants.sql
│       └── fraud_signals.sql
│
├── spark/
│   ├── jobs/
│   │   ├── bronze_ingest.py      # raw → s3a://bronze/<tabla>/dt=YYYY-MM-DD/
│   │   ├── silver_clean.py       # cast, UTC, FK, FX, blacklist join
│   │   └── gold_marts.py         # facts + dims + agregaciones → Postgres
│   └── common/
│       ├── spark_session.py      # builder reutilizable (S3A, JDBC)
│       ├── io.py                 # helpers read/write parquet + jdbc
│       └── schemas.py            # StructType explícitos por tabla
│
├── expectations/
│   ├── great_expectations.yml
│   ├── expectations/
│   │   ├── bronze_payments.json
│   │   ├── silver_payments.json
│   │   └── gold_fct_payments.json
│   └── checkpoints/
│
├── airflow/
│   ├── dags/
│   │   └── aurelia_daily.py      # DAG diario end-to-end
│   ├── plugins/                  # vacío por ahora
│   └── requirements.txt          # deps que Airflow necesita en runtime
│
├── blacklist_api/
│   ├── main.py                   # FastAPI: GET /v1/blacklist
│   └── blacklist.json            # fuente de verdad del mock
│
└── tests/
    ├── conftest.py               # fixtures: SparkSession local, tmp paths
    ├── test_generate_data.py
    ├── test_silver_transforms.py
    ├── test_gold_marts.py
    ├── test_blacklist_api.py
    └── data/                     # fixtures pequeños de CSV/parquet
```

---

## Servicios en `docker-compose.yml`

| Servicio | Imagen | Puerto host | Rol |
|---|---|---|---|
| `postgres-dwh` | `postgres:15` | 5432 | DWH gold. Monta `warehouse/ddl/` en `/docker-entrypoint-initdb.d/` |
| `postgres-airflow` | `postgres:15` | 5433 | Metadata de Airflow |
| `minio` | `minio/minio` | 9000, 9001 | Object storage S3-compatible (bronze/silver) |
| `minio-init` | `minio/mc` | — | Crea buckets `bronze`, `silver`, `gold` y sale |
| `airflow-init` | `apache/airflow:2.9-python3.11` | — | `airflow db migrate` + user admin |
| `airflow-webserver` | idem | 8080 | UI |
| `airflow-scheduler` | idem | — | Ejecuta el DAG |
| `blacklist-api` | build local (FastAPI) | 8000 | Mock del endpoint externo |

Ejecutor Airflow: **LocalExecutor** (más liviano que Celery, alcanza para el DAG).
Spark: **local mode** dentro del container de Airflow (no cluster). Menor RAM, más simple, cero orquestación extra.

---

## DAG `aurelia_daily.py`

```
generate_data_if_missing
    ↓
[ingest_bronze_customers, ingest_bronze_merchants, ingest_bronze_payments,
 ingest_bronze_chargebacks, ingest_bronze_fx, ingest_bronze_events,
 ingest_bronze_blacklist_api]                    ← paralelo
    ↓
validate_bronze (Great Expectations checkpoint)
    ↓
transform_silver
    ↓
validate_silver
    ↓
build_gold  (escribe a Postgres vía JDBC, mode="overwrite" por partición)
    ↓
validate_gold
```

- `schedule="@daily"`, `catchup=False`, `max_active_runs=1`.
- Todas las tasks reciben `{{ ds }}` y particionan por `dt=` → **idempotencia** (re-run pisa la misma partición).
- Blacklist se ingiere via `httpx` hacia `http://blacklist-api:8000/v1/blacklist` en la task del bronze.

---

## Modelo de datos gold (`warehouse/ddl/001_init.sql`)

- `dim_customer(customer_id PK, signup_date, country, marketing_channel, cohort_month)`
- `dim_merchant(merchant_id PK, category, risk_level, is_blacklisted, created_at)`
- `dim_date(date_key PK, year, month, day, week, quarter)`
- `fct_payments(payment_id PK, customer_id FK, merchant_id FK, date_key FK, amount_usd, status, method, created_at_utc, has_chargeback, chargeback_reason)`
- `agg_daily_kpis(date_key PK, total_payments, captured_payments, auth_rate, chargeback_rate, gross_volume_usd)`
- `agg_cohort_ltv(cohort_month, months_since_signup, avg_ltv_usd, active_customers)`

Todos con `CREATE TABLE IF NOT EXISTS` para no romper reruns.

---

## Great Expectations — suites mínimas

**bronze_payments:** columnas presentes, `payment_id` no nulo/único, `amount` > 0.
**silver_payments:** `status` en dominio {captured, refused, refunded, pending}, `currency` ISO válida, `created_at_utc` no futura, FK a customer/merchant válidas.
**gold_fct_payments:** `amount_usd` > 0, `auth_rate` ∈ [0, 1] en agregados, no nulos en PKs.

Cada suite se corre como task bloqueante en el DAG (falla el pipeline si falla la suite).

---

## Queries de negocio (`warehouse/queries/`)

1. **auth_rate_daily.sql** — tasa de aprobación por día.
2. **chargeback_rate.sql** — % de payments con chargeback por merchant y por período.
3. **ltv_by_cohort.sql** — LTV acumulado por cohorte mensual de signup.
4. **high_risk_merchants.sql** — merchants con chargeback_rate > 2% o categoría crypto/high risk.
5. **fraud_signals.sql** — cruce de payments + eventos `failed_pin` + blacklist.

---

## Tests con pytest

- **`test_generate_data.py`** — el seed produce todos los archivos y respeta schemas.
- **`test_silver_transforms.py`** — fixtures Spark chicos (10 rows), verifica cast UTC, conversión FX, join blacklist.
- **`test_gold_marts.py`** — auth_rate = captured/total, chargeback_rate cuadra manualmente en un caso conocido.
- **`test_blacklist_api.py`** — `TestClient` de FastAPI, valida shape del JSON.

Estrategia: **integration light**. No mockeamos Spark; usamos SparkSession local en `conftest.py` con `session-scoped fixture`. DataFrames chicos in-memory, sin dependencia de MinIO ni Postgres.

---

## README.md (siguiendo la checklist del profesor, en tercera persona)

1. Título + descripción breve (1 párrafo).
2. Badges (si hay tiempo para CI: GitHub Actions + coverage).
3. **Features** — qué produce el pipeline (métricas, capas, calidad).
4. **Prerequisitos** — Docker Desktop, puertos libres (5432, 5433, 8000, 8080, 9000, 9001), 6 GB RAM disponibles.
5. **Cómo correr** — `docker compose up --build` + esperar a que Airflow marque healthy + abrir `http://localhost:8080` (admin/admin) + trigger manual del DAG.
6. **Cómo correr los tests** — `docker compose run --rm airflow-scheduler pytest` (o local con `uv run pytest`).
7. **Estructura del repo** (árbol).
8. **Modelo de datos** — link a ARCHITECTURE.md.
9. **Decisiones y trade-offs**:
   - Spark local vs cluster (por qué local).
   - Sin Alembic (schema gold es owned por el pipeline, DDL en SQL puro).
   - LocalExecutor vs Celery.
   - PySpark vs dbt.
10. **Áreas de mejora / Future work** — Kafka streaming, Terraform, dbt sobre gold, tests de contract, deploy real.
11. **Env vars** — referencia a `.env.example`.
12. **Capturas** — DAG verde en Airflow UI, MinIO console con buckets poblados, DBeaver con tablas gold.

---

## ARCHITECTURE.md

- Diagrama Mermaid con: fuentes → bronze (MinIO) → silver (MinIO) → gold (Postgres) → queries.
- Diagrama de servicios Docker.
- Explicación de la medallion architecture aplicada.
- Diagrama de secuencia del DAG.

---

## Orden de ejecución (fases con checkpoints)

| Fase | Descripción | Duración | Checkpoint |
|---|---|---|---|
| 0 | Scaffolding del repo, `.env.example`, `.gitignore`, `pyproject.toml`, README stub | 30 min | Repo estructurado, commit inicial |
| 1 | `docker-compose.yml` con los 8 servicios + healthchecks. **Pausa: instalar Docker Desktop.** | 1.5 h | `docker compose up` levanta todo verde |
| 2 | `scripts/generate_data.py` + `warehouse/ddl/001_init.sql` + `blacklist_api/` FastAPI | 1 h | Datos en `/data/raw/`, blacklist API responde, Postgres tiene tablas |
| 3 | Jobs Spark: bronze → silver → gold (correr standalone primero, sin Airflow) | 2.5 h | Parquet en MinIO, filas en Postgres |
| 4 | DAG Airflow que orquesta los jobs | 1 h | DAG corre verde end-to-end desde la UI |
| 5 | Great Expectations: suites + checkpoints + integración al DAG | 1 h | Validaciones bloquean el DAG si fallan |
| 6 | Queries SQL de negocio en `warehouse/queries/` | 45 min | Cada query devuelve resultados coherentes |
| 7 | Tests pytest | 45 min | `pytest` verde |
| 8 | README + ARCHITECTURE + capturas | 45 min | Repo listo para review |

**Total:** ~9 h (dentro del rango 8–10 h del PDF).

---

## Criterios del PDF y cómo los cubre el plan

| Criterio | Cubierto por |
|---|---|
| Diseño arquitectónico | Medallion + separación por capas + ARCHITECTURE.md |
| Calidad de código | Módulos chicos, schemas explícitos, sin lógica en DAG (solo orquestación) |
| Data Quality | Great Expectations en 3 capas + tasks bloqueantes |
| Idempotencia y reproducibilidad | Particionado por `dt=`, `CREATE IF NOT EXISTS`, seed determinista con seed fijo, `catchup=False` |
| Modelo analítico y DWH | Star schema (facts + dims + aggs) con DDL versionado |
| Documentación | README con checklist del profesor + ARCHITECTURE + capturas |
| Extras | Kafka y Terraform documentados como future work (fuera de scope por timebox) |
