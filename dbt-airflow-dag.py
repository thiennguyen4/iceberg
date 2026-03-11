from __future__ import annotations

import time
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DBT_PROJECT_DIR = "/opt/dbt/dbt_iceberg"
OPENMETADATA_URL = "http://openmetadata:8585/api"
OPENMETADATA_SERVICE_NAME = "iceberg-hive-catalog"

SPARK_THRIFT_HOST = "spark-iceberg"
SPARK_THRIFT_PORT = 10000

INGESTION_WAIT_SEC = 180
INGESTION_POLL_SEC = 10

DEFAULT_ARGS = {
    "owner": "iceburg",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

SPARK_THRIFT_CONF = {
    "spark.sql.extensions": (
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    ),
    "spark.sql.catalog.demo": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.demo.catalog-impl": "org.apache.iceberg.hive.HiveCatalog",
    "spark.sql.catalog.demo.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.demo.warehouse": "s3a://warehouse/",
    "spark.sql.catalog.demo.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.demo.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.demo.s3.access-key-id": "admin",
    "spark.sql.catalog.demo.s3.secret-access-key": "password",
    "spark.sql.catalog.demo.s3.path-style-access": "true",
    "spark.sql.defaultCatalog": "demo",
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.iceberg.handle-timestamp-without-timezone": "true",
    "spark.sql.catalog.demo.cache-enabled": "false",
}

ICEBERG_NAMESPACES = ["sales", "dbt_staging", "dbt_mart"]


def _execute_sqls(sqls: list[str], database: str | None = None) -> None:
    """Open a Spark Thrift connection and execute each SQL statement in order."""
    from pyhive import hive

    kwargs = dict(
        host=SPARK_THRIFT_HOST,
        port=SPARK_THRIFT_PORT,
        configuration=SPARK_THRIFT_CONF,
    )
    if database is not None:
        kwargs["database"] = database

    conn = hive.connect(**kwargs)
    cursor = conn.cursor()
    for sql in sqls:
        print(f"Executing: {sql.strip()[:80]}...")
        cursor.execute(sql)
    cursor.close()
    conn.close()


def seed_orders(**_) -> None:
    """Create the Iceberg sales.orders table and insert sample rows via Spark Thrift."""
    _execute_sqls(
        [
            *[f"CREATE NAMESPACE IF NOT EXISTS demo.{ns}" for ns in ICEBERG_NAMESPACES],
            """
            CREATE TABLE IF NOT EXISTS demo.sales.orders (
                order_id        STRING,
                customer_id     STRING,
                product_name    STRING,
                quantity        INT,
                price           DECIMAL(10, 2),
                order_timestamp TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(order_timestamp))
            """,
            """
            INSERT OVERWRITE demo.sales.orders VALUES
            ('ORD-001', 'CUST-101', 'Laptop',      2, 999.99,  TIMESTAMP '2026-02-01 10:00:00'),
            ('ORD-002', 'CUST-102', 'Mouse',        5,  29.99,  TIMESTAMP '2026-02-01 11:00:00'),
            ('ORD-003', 'CUST-103', 'Keyboard',     1,  79.99,  TIMESTAMP '2026-02-02 09:00:00'),
            ('ORD-004', 'CUST-101', 'Monitor',      1, 399.99,  TIMESTAMP '2026-02-02 14:00:00'),
            ('ORD-005', 'CUST-104', 'USB Hub',      3,  19.99,  TIMESTAMP '2026-02-03 08:00:00'),
            ('ORD-006', 'CUST-102', 'Webcam',       1,  89.99,  TIMESTAMP '2026-02-03 10:00:00'),
            ('ORD-007', 'CUST-105', 'Headphones',   1, 149.99,  TIMESTAMP '2026-02-03 12:00:00'),
            ('ORD-008', 'CUST-103', 'Desk Lamp',    2,  34.99,  TIMESTAMP '2026-02-04 09:30:00')
            """,
        ]
    )
    print("Seeded demo.sales.orders successfully")


def _get_auth_token() -> str:
    """Authenticate against OpenMetadata and return a Bearer access token."""
    resp = requests.post(
        f"{OPENMETADATA_URL}/v1/users/login",
        json={"email": "admin@open-metadata.org", "password": "YWRtaW4="},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["accessToken"]


def _get_ingestion_pipeline(headers: dict, pipeline_type: str) -> dict:
    """Fetch the ingestion pipeline of a given type from OpenMetadata; raise if not found."""
    resp = requests.get(
        f"{OPENMETADATA_URL}/v1/services/ingestionPipelines"
        f"?service={OPENMETADATA_SERVICE_NAME}&limit=25",
        headers=headers,
        timeout=10,
    )
    resp.raise_for_status()
    pipelines = resp.json().get("data", [])

    pipeline = next(
        (p for p in pipelines if p.get("pipelineType") == pipeline_type),
        None,
    )

    if not pipeline:
        raise RuntimeError(
            f"No '{pipeline_type}' ingestion pipeline found for service '{OPENMETADATA_SERVICE_NAME}'. "
            f"Create it in OpenMetadata UI first."
        )
    return pipeline


def _trigger_ingestion_and_wait(headers: dict, pipeline_id: str, pipeline_type: str) -> None:
    """Trigger an OpenMetadata ingestion pipeline and poll until it succeeds or times out."""
    trigger_resp = requests.post(
        f"{OPENMETADATA_URL}/v1/services/ingestionPipelines/trigger/{pipeline_id}",
        headers=headers,
        timeout=10,
    )
    trigger_resp.raise_for_status()
    print(f"Triggered {pipeline_type} ingestion pipeline: {pipeline_id}")

    time.sleep(20)
    elapsed = 20
    start_window_ms = int((time.time() - 30) * 1000)

    while elapsed < INGESTION_WAIT_SEC:
        time.sleep(INGESTION_POLL_SEC)
        elapsed += INGESTION_POLL_SEC

        run_state = _get_ingestion_run_state(headers, pipeline_id, start_window_ms)
        print(f"[{pipeline_type}] status after {elapsed}s: {run_state}")

        if run_state in ("success", "partialsuccess"):
            print(f"[{pipeline_type}] ingestion completed.")
            return
        if run_state == "failed":
            raise RuntimeError(f"[{pipeline_type}] ingestion pipeline failed.")

    print(f"[{pipeline_type}] did not complete within {INGESTION_WAIT_SEC}s, proceeding anyway.")


def _get_ingestion_run_state(headers: dict, pipeline_id: str, start_window_ms: int) -> str:
    """Poll the latest pipeline run state from OpenMetadata using two fallback endpoints."""
    for url in [
        f"{OPENMETADATA_URL}/v1/services/ingestionPipelines/{pipeline_id}/lastIngestionRuns",
        f"{OPENMETADATA_URL}/v1/services/ingestionPipelines/{pipeline_id}"
        f"/pipelineStatus?startTs={start_window_ms}&endTs={int(time.time() * 1000)}&limit=1",
    ]:
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                continue
            data = resp.json()
            runs = (
                data.get("data", [])
                if isinstance(data, dict) and "data" in data
                else data if isinstance(data, list)
                else []
            )
            if runs:
                latest = sorted(
                    runs,
                    key=lambda x: x.get("timestamp", x.get("startDate", 0)),
                    reverse=True,
                )[0]
                return latest.get("pipelineState", "").lower()
        except Exception:
            continue

    try:
        resp = requests.get(
            f"{OPENMETADATA_URL}/v1/services/ingestionPipelines/{pipeline_id}"
            "?fields=pipelineStatuses",
            headers=headers,
            timeout=10,
        )
        if resp.status_code == 200:
            statuses = resp.json().get("pipelineStatuses", [])
            if statuses:
                return statuses[-1].get("pipelineState", "").lower()
    except Exception:
        pass

    return ""


def trigger_metadata_ingestion(**_) -> None:
    """Authenticate, locate the metadata ingestion pipeline, and trigger it."""
    token = _get_auth_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    pipeline = _get_ingestion_pipeline(headers, "metadata")
    _trigger_ingestion_and_wait(headers, pipeline["id"], "metadata")


def trigger_dbt_ingestion(**_) -> None:
    """Authenticate, locate the dbt ingestion pipeline, and trigger it."""
    token = _get_auth_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    pipeline = _get_ingestion_pipeline(headers, "dbt")
    _trigger_ingestion_and_wait(headers, pipeline["id"], "dbt")


@dag(
    dag_id="dbt_pipeline",
    default_args=DEFAULT_ARGS,
    description="Seed raw orders → dbt staging & mart transforms → OpenMetadata lineage ingestion",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "hive", "iceberg", "lineage"],
)
def dbt_pipeline():
    seed_raw_orders = PythonOperator(
        task_id="seed_raw_orders",
        python_callable=seed_orders,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select staging --profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --select staging --profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_run_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select mart --profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_test_mart = BashOperator(
        task_id="dbt_test_mart",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --select mart --profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt docs generate --profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    metadata_ingestion = PythonOperator(
        task_id="trigger_metadata_ingestion",
        python_callable=trigger_metadata_ingestion,
    )

    dbt_ingestion = PythonOperator(
        task_id="trigger_dbt_ingestion",
        python_callable=trigger_dbt_ingestion,
    )

    (
        seed_raw_orders
        >> dbt_deps
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_mart
        >> dbt_test_mart
        >> dbt_docs
        >> metadata_ingestion
        >> dbt_ingestion
    )


dbt_pipeline()
