from typing import Optional

import urllib.request
import json
import urllib.error
import os

BASE_URL = os.getenv("OPENMETADATA_URL", "http://localhost:8585/api")
SERVICE_NAME = "iceberg-hive-catalog"


def get_auth_token() -> str:
    """Authenticate against OpenMetadata and return a Bearer access token."""
    req = urllib.request.Request(
        f"{BASE_URL}/v1/users/login",
        data=json.dumps({"email": "admin@open-metadata.org", "password": "YWRtaW4="}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    resp = urllib.request.urlopen(req, timeout=15)
    return json.loads(resp.read())["accessToken"]


def get_or_create_service(token: str) -> str:
    """Return the database service id, creating the Iceberg Hive service if it does not exist."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    try:
        req = urllib.request.Request(
            f"{BASE_URL}/v1/services/databaseServices/name/{SERVICE_NAME}",
            headers=headers,
        )
        resp = urllib.request.urlopen(req, timeout=15)
        svc = json.loads(resp.read())
        print(f"[OK] Service '{SERVICE_NAME}' found: id={svc['id']}")
        return svc["id"]
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise

    print(f"[INFO] Service '{SERVICE_NAME}' not found, creating...")
    payload = {
        "name": SERVICE_NAME,
        "displayName": "Iceberg Hive Catalog",
        "serviceType": "Iceberg",
        "connection": {
            "config": {
                "type": "Iceberg",
                "catalog": {
                    "name": "hive",
                    "connection": {
                        "type": "HiveCatalogConnection",
                        "metastoreUri": "thrift://hive-metastore:9083",
                        "warehouse": "s3a://warehouse/",
                    },
                },
                "fileSystem": {
                    "securityConfig": {
                        "awsAccessKeyId": "admin",
                        "awsSecretAccessKey": "password",
                        "awsRegion": "us-east-1",
                    },
                    "endPointURL": "http://minio:9000",
                },
                "connectionOptions": {
                    "s3.endpoint": "http://minio:9000",
                    "s3.access-key-id": "admin",
                    "s3.secret-access-key": "password",
                    "s3.path-style-access": "true",
                    "client.region": "us-east-1",
                },
            }
        },
    }
    req = urllib.request.Request(
        f"{BASE_URL}/v1/services/databaseServices",
        data=json.dumps(payload).encode(),
        headers=headers,
        method="POST",
    )
    resp = urllib.request.urlopen(req, timeout=15)
    svc = json.loads(resp.read())
    print(f"[OK] Service '{SERVICE_NAME}' created: id={svc['id']}")
    return svc["id"]


def find_pipeline_id(token: str, pipeline_type: str) -> Optional[str]:
    """Return the id of an existing ingestion pipeline matching pipeline_type, or None."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    req = urllib.request.Request(
        f"{BASE_URL}/v1/services/ingestionPipelines?service={SERVICE_NAME}&limit=25",
        headers=headers,
    )
    resp = urllib.request.urlopen(req, timeout=15)
    pipelines = json.loads(resp.read()).get("data", [])
    match = next((p for p in pipelines if p.get("pipelineType") == pipeline_type), None)
    return match["id"] if match else None


def create_or_update_pipeline(token: str, service_id: str, pipeline_type: str, pipeline_name: str, source_config: dict) -> None:
    """Delete any existing pipeline of the given type, then create a fresh one."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    payload = {
        "name": pipeline_name,
        "pipelineType": pipeline_type,
        "service": {"id": service_id, "type": "databaseService"},
        "sourceConfig": {"config": source_config},
        "airflowConfig": {"scheduleInterval": None},
    }

    existing_id = find_pipeline_id(token, pipeline_type)
    if existing_id:
        del_req = urllib.request.Request(
            f"{BASE_URL}/v1/services/ingestionPipelines/{existing_id}?hardDelete=true",
            headers=headers,
            method="DELETE",
        )
        urllib.request.urlopen(del_req, timeout=15)
        print(f"[INFO] Deleted old '{pipeline_type}' pipeline, recreating...")

    req = urllib.request.Request(
        f"{BASE_URL}/v1/services/ingestionPipelines",
        data=json.dumps(payload).encode(),
        headers=headers,
        method="POST",
    )

    try:
        resp = urllib.request.urlopen(req, timeout=15)
        result = json.loads(resp.read())
        print(f"[OK] Created '{pipeline_type}' pipeline: {result['name']} | id={result['id']}")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"[ERR] '{pipeline_type}' pipeline HTTP {e.code}: {body[:300]}")


def main():
    """Bootstrap OpenMetadata: create the Iceberg service and both ingestion pipelines."""
    print("=== OpenMetadata Ingestion Pipeline Setup ===")
    print(f"    Target service: {SERVICE_NAME}")

    print("\n[1] Logging in...")
    token = get_auth_token()
    print(f"    Token OK: {token[:25]}...")

    print(f"\n[2] Getting or creating service '{SERVICE_NAME}'...")
    service_id = get_or_create_service(token)

    print("\n[3] Creating/updating metadata pipeline...")
    create_or_update_pipeline(
        token,
        service_id,
        "metadata",
        f"{SERVICE_NAME}_metadata",
        {
            "type": "DatabaseMetadata",
            "markDeletedTables": False,
            "includeTables": True,
            "includeViews": True,
        },
    )

    print("\n[4] Creating/updating dbt pipeline...")
    create_or_update_pipeline(
        token,
        service_id,
        "dbt",
        f"{SERVICE_NAME}_dbt",
        {
            "type": "DBT",
            "dbtConfigSource": {
                "dbtCatalogFilePath": "/opt/dbt/dbt_iceberg/target/catalog.json",
                "dbtManifestFilePath": "/opt/dbt/dbt_iceberg/target/manifest.json",
                "dbtRunResultsFilePath": "/opt/dbt/dbt_iceberg/target/run_results.json",
            },
            "dbtUpdateDescriptions": True,
            "dbtUpdateOwners": True,
            "includeTags": True,
            "searchAcrossDatabases": True,
        },
    )

    print("\n=== Done. Trigger DAG in Airflow UI: http://localhost:8080 ===")
    print(f"=== Check OpenMetadata UI: http://localhost:8585 ===")


if __name__ == "__main__":
    main()

