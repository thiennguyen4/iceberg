# DBT Flow — How It Works

## Architecture Overview

```
MinIO (S3)
  └── warehouse/          ← Iceberg data files (Parquet)

Hive Metastore (port 9083)
  └── Manages table schema & partition metadata

Spark Iceberg (port 10000 Thrift)
  └── Executes SQL on Iceberg tables

dbt (inside openmetadata-ingestion container)
  └── Reads from demo.sales.orders  (Hive catalog)
  └── Writes dbt_staging.stg_orders
  └── Writes dbt_mart.mart_daily_revenue

Airflow (inside openmetadata-ingestion, port 8080)
  └── Orchestrates the full pipeline via DAG: dbt_pipeline

OpenMetadata (port 8585)
  └── Stores table metadata + dbt lineage graph
```

---

## Data Flow Step by Step

```
seed_raw_orders
  │  Creates namespaces: demo.sales, demo.dbt_staging, demo.dbt_mart
  │  Creates demo.sales.orders (Iceberg table via Spark Thrift)
  │  Inserts 8 sample order rows
  ▼
dbt_deps
  │  Installs dbt packages (dbt_utils, etc.) declared in packages.yml
  ▼
dbt_run_staging
  │  Runs models/staging/stg_orders.sql
  │  Reads from demo.sales.orders
  │  Cleans nulls, trims strings, validates quantity > 0 and price > 0
  │  Writes result to demo.dbt_staging.stg_orders (Iceberg table)
  ▼
dbt_test_staging
  │  Runs schema tests defined in models/staging/schema.yml
  │  Tests: not_null, unique on order_id; not_null on customer_id, price, quantity
  ▼
dbt_run_mart
  │  Runs models/mart/mart_daily_revenue.sql
  │  Reads from demo.dbt_staging.stg_orders
  │  Aggregates by day: total_orders, unique_customers, total_revenue, avg/min/max order value
  │  Writes to demo.dbt_mart.mart_daily_revenue (Iceberg table, Parquet + ZSTD)
  ▼
dbt_test_mart
  │  Runs schema tests defined in models/mart/schema.yml
  │  Tests: not_null + unique on order_date; not_null on total_orders, total_revenue
  ▼
dbt_docs_generate
  │  Generates dbt docs artifacts: catalog.json, manifest.json, run_results.json
  │  Saved to /opt/dbt/dbt_iceberg/target/
  ▼
trigger_metadata_ingestion
  │  Authenticates to OpenMetadata
  │  Triggers the "metadata" ingestion pipeline for service iceberg-hive-catalog
  │  Polls every 10s (max 180s) until success
  │  Crawls Hive Metastore → syncs all Iceberg table schemas into OpenMetadata
  ▼
trigger_dbt_ingestion
     Authenticates to OpenMetadata
     Triggers the "dbt" ingestion pipeline for service iceberg-hive-catalog
     Polls every 10s (max 180s) until success
     Reads catalog.json + manifest.json + run_results.json
     Pushes dbt lineage graph into OpenMetadata
     Result: sales.orders → stg_orders → mart_daily_revenue visible as lineage
```

---

## Starting the Stack

### 1. Start all services

```bash
docker compose up -d
```

Wait for all containers to be healthy (≈ 3–5 minutes):

| Container | Ready signal |
|---|---|
| `hive-postgres` | healthcheck passes |
| `hive-metastore` | port 9083 open |
| `spark-iceberg` | Thrift on port 10000 |
| `openmetadata-elasticsearch` | cluster status yellow |
| `openmetadata-mysql` | mysqladmin ping |
| `openmetadata` | port 8585 reachable |
| `openmetadata-ingestion` | `/health` returns 200 |

### 2. Bootstrap OpenMetadata service + ingestion pipelines

Run once after the first `docker compose up`:

```bash
docker exec openmetadata-ingestion python /opt/om_create_pipelines.py
```

This creates:
- The **Iceberg Hive Catalog** database service in OpenMetadata
- The **metadata** ingestion pipeline (crawls table schemas)
- The **dbt** ingestion pipeline (imports lineage from dbt artifacts)

---

## Triggering the DBT Pipeline

### Via Airflow UI (recommended)

1. Open **http://localhost:8080**
2. Login: `admin` / `admin`
3. Find DAG **`dbt_pipeline`**
4. Click **▶ Trigger DAG**
5. Monitor task progress in the Graph or Grid view

### Via Airflow CLI

```bash
docker exec openmetadata-ingestion airflow dags trigger dbt_pipeline
```

---

## Viewing DBT Lineage in OpenMetadata

### Prerequisites
- The `dbt_pipeline` DAG must have completed successfully at least once
- Both `trigger_metadata_ingestion` and `trigger_dbt_ingestion` tasks must show **success**

### Steps

1. Open **http://localhost:8585**
2. Login: `admin@open-metadata.org` / `admin`
3. Go to **Explore → Tables**
4. Search for `mart_daily_revenue`
5. Click on the table → open the **Lineage** tab

You will see the full lineage graph:

```
demo.sales.orders  →  dbt_staging.stg_orders  →  dbt_mart.mart_daily_revenue
```

### Checking dbt descriptions and tags

- Each table column shows descriptions sourced from `schema.yml`
- dbt tags and model descriptions are pulled from `manifest.json`
- Go to **Explore → Tables** → filter by service `iceberg-hive-catalog`

---

## Key Files Reference

| File | Purpose |
|---|---|
| `dbt-airflow-dag.py` | Airflow DAG — full pipeline definition |
| `om_create_pipelines.py` | One-time bootstrap: creates OM service + pipelines |
| `dbt/models/staging/stg_orders.sql` | Staging model: cleans raw orders |
| `dbt/models/mart/mart_daily_revenue.sql` | Mart model: daily revenue aggregation |
| `dbt/models/staging/sources.yml` | Declares `demo.sales.orders` as dbt source |
| `dbt/models/staging/schema.yml` | Tests + descriptions for stg_orders |
| `dbt/models/mart/schema.yml` | Tests + descriptions for mart_daily_revenue |
| `dbt/dbt_project.yml` | dbt project config, schema targets |
| `dbt/profiles.yml` | Spark Thrift connection profile |
| `om-metadata-ingestion.yaml` | Metadata ingestion config (used at pipeline creation) |
| `om-dbt-ingestion.yaml` | DBT ingestion config (used at pipeline creation) |
| `docker-compose.yml` | Full service topology |

---

## Troubleshooting

| Symptom | Action |
|---|---|
| DAG not visible in Airflow | Wait 30s for DAG file to be parsed; check `docker logs openmetadata-ingestion` |
| `seed_raw_orders` fails | Check that `spark-iceberg` container is healthy and port 10000 is open |
| `dbt_run_staging` fails | Check Spark Thrift connectivity from ingestion container; verify `hive-metastore` is up |
| Lineage not showing in OpenMetadata | Ensure `trigger_dbt_ingestion` succeeded; check `dbt/target/manifest.json` exists |
| OpenMetadata pipelines not found | Re-run `python /opt/om_create_pipelines.py` inside the ingestion container |

