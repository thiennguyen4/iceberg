from __future__ import annotations

import time
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DBT_PROJECT_DIR = "/opt/dbt/nessie_transform"
OPENMETADATA_URL = "http://openmetadata:8585/api"
SPARK_THRIFT_HOST = "spark-iceberg"
SPARK_THRIFT_PORT = 10000

DEFAULT_ARGS = {
    "owner": "iceburg",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

OM_SERVICE_NAME = "iceberg-rest-catalog"
OM_INGESTION_WAIT_SEC = 120
OM_INGESTION_POLL_SEC = 5

SPARK_THRIFT_CONF = {
    "spark.sql.extensions": (
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
    ),
    "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
    "spark.sql.catalog.nessie.uri": "http://nessie:19120/api/v2",
    "spark.sql.catalog.nessie.ref": "main",
    "spark.sql.catalog.nessie.warehouse": "s3://nessie/",
    "spark.sql.catalog.nessie.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.nessie.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.nessie.s3.access-key-id": "admin",
    "spark.sql.catalog.nessie.s3.secret-access-key": "password",
    "spark.sql.catalog.nessie.s3.path-style-access": "true",
    "spark.sql.defaultCatalog": "nessie",
}

def _execute_sqls(sqls: list[str], database: str | None = None) -> None:
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


NESSIE_API_URL = "http://nessie:19120/api/v2"
NESSIE_BRANCH = "main"
NESSIE_NAMESPACES = ["default", "dbt_staging", "dbt_mart"]

def seed_nessie_orders(**_) -> None:
    namespace_sqls = [
        f"CREATE NAMESPACE IF NOT EXISTS nessie.{ns}"
        for ns in NESSIE_NAMESPACES
    ]
    _execute_sqls(namespace_sqls)

    _execute_sqls(
        [
            """
            CREATE TABLE IF NOT EXISTS nessie.default.orders (
                                                                 order_id    BIGINT,
                                                                 customer_id BIGINT,
                                                                 order_time  TIMESTAMP,
                                                                 amount      DECIMAL(10, 2),
                status      STRING,
                created_at  TIMESTAMP
                )
                USING iceberg
                PARTITIONED BY (days(order_time))
            """,
            """
            INSERT OVERWRITE nessie.default.orders VALUES
            (1, 101, TIMESTAMP '2026-02-01 10:00:00', 100.50, 'completed', current_timestamp()),
            (2, 102, TIMESTAMP '2026-02-01 11:00:00', 200.00, 'completed', current_timestamp()),
            (3, 103, TIMESTAMP '2026-02-02 09:00:00', 150.75, 'pending',   current_timestamp()),
            (4, 101, TIMESTAMP '2026-02-02 14:00:00', 300.00, 'completed', current_timestamp()),
            (5, 104, TIMESTAMP '2026-02-03 08:00:00',  50.00, 'cancelled', current_timestamp()),
            (6, 102, TIMESTAMP '2026-02-03 10:00:00', 175.25, 'completed', current_timestamp()),
            (7, 105, TIMESTAMP '2026-02-03 12:00:00', 225.00, 'completed', current_timestamp()),
            (8, 103, TIMESTAMP '2026-02-04 09:30:00',  80.00, 'pending',   current_timestamp())
            """,
        ]
    )
    print("Seeded nessie.default.orders successfully")

def _get_om_token() -> str:
    resp = requests.post(
        f"{OPENMETADATA_URL}/v1/users/login",
        json={"email": "admin@open-metadata.org", "password": "YWRtaW4="},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["accessToken"]


def _get_pipeline_by_type(headers: dict, pipeline_type: str) -> dict:
    resp = requests.get(
        f"{OPENMETADATA_URL}/v1/services/ingestionPipelines"
        f"?service={OM_SERVICE_NAME}&limit=25",
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
            f"No '{pipeline_type}' ingestion pipeline found for service '{OM_SERVICE_NAME}'. "
            f"Create it in OpenMetadata UI first."
        )
    return pipeline


def _trigger_and_wait(headers: dict, pipeline_id: str, pipeline_type: str) -> None:
    requests.post(
        f"{OPENMETADATA_URL}/v1/services/ingestionPipelines/trigger/{pipeline_id}",
        headers=headers,
        timeout=10,
    ).raise_for_status()
    print(f"Triggered {pipeline_type} ingestion pipeline: {pipeline_id}")

    elapsed = 0
    while elapsed < OM_INGESTION_WAIT_SEC:
        time.sleep(OM_INGESTION_POLL_SEC)
        elapsed += OM_INGESTION_POLL_SEC

        status_resp = requests.get(
            f"{OPENMETADATA_URL}/v1/services/ingestionPipelines/{pipeline_id}",
            headers=headers,
            timeout=10,
        )
        status_resp.raise_for_status()
        run_state = status_resp.json().get("pipelineStatuses", {}).get("pipelineState", "")
        print(f"[{pipeline_type}] status after {elapsed}s: {run_state}")

        if run_state in ("success", "partialSuccess"):
            print(f"[{pipeline_type}] ingestion completed.")
            return
        if run_state == "failed":
            raise RuntimeError(f"[{pipeline_type}] ingestion pipeline {pipeline_id} failed.")

    print(f"[{pipeline_type}] did not complete within {OM_INGESTION_WAIT_SEC}s, proceeding anyway.")


def trigger_om_metadata_ingestion(**_) -> None:
    token = _get_om_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    pipeline = _get_pipeline_by_type(headers, "metadata")
    _trigger_and_wait(headers, pipeline["id"], "metadata")


def trigger_om_dbt_ingestion(**_) -> None:
    token = _get_om_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    pipeline = _get_pipeline_by_type(headers, "dbt")
    _trigger_and_wait(headers, pipeline["id"], "dbt")


@dag(
    dag_id="dbt_nessie_transform_lineage",
    default_args=DEFAULT_ARGS,
    description="dbt transform Nessie Iceberg tables + auto lineage via OpenMetadata dbt ingestion",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "nessie", "iceberg", "lineage"],
)
def dbt_nessie_pipeline():
    seed_nessie_data = PythonOperator(
        task_id="seed_nessie_raw_orders",
        python_callable=seed_nessie_orders,
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

    om_metadata_ingestion = PythonOperator(
        task_id="trigger_om_metadata_ingestion",
        python_callable=trigger_om_metadata_ingestion,
    )

    om_dbt_ingestion = PythonOperator(
        task_id="trigger_om_dbt_ingestion",
        python_callable=trigger_om_dbt_ingestion,
    )

    (
        seed_nessie_data
        >> dbt_deps
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_mart
        >> dbt_test_mart
        >> dbt_docs
        >> om_metadata_ingestion
        >> om_dbt_ingestion
    )


dbt_nessie_pipeline()
