import urllib.request
import json
import urllib.error
import sys

BASE = "http://localhost:8585/api"


def login():
    req = urllib.request.Request(
        f"{BASE}/v1/users/login",
        data=json.dumps({"email": "admin@open-metadata.org", "password": "YWRtaW4="}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    resp = urllib.request.urlopen(req, timeout=15)
    return json.loads(resp.read())["accessToken"]


def get_service_id(token):
    req = urllib.request.Request(
        f"{BASE}/v1/services/databaseServices/name/iceberg-rest-catalog",
        headers={"Authorization": f"Bearer {token}"},
    )
    resp = urllib.request.urlopen(req, timeout=15)
    svc = json.loads(resp.read())
    return svc["id"]


def create_pipeline(token, svc_id, p_type, p_name, source_config):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    payload = {
        "name": p_name,
        "pipelineType": p_type,
        "service": {"id": svc_id, "type": "databaseService"},
        "sourceConfig": {"config": source_config},
        "airflowConfig": {"scheduleInterval": None},
    }
    req = urllib.request.Request(
        f"{BASE}/v1/services/ingestionPipelines",
        data=json.dumps(payload).encode(),
        headers=headers,
        method="POST",
    )
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        result = json.loads(resp.read())
        print(f"[OK] Created '{p_type}' pipeline: {result['name']} | id={result['id']}")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        if e.code == 409:
            print(f"[SKIP] '{p_type}' pipeline already exists")
        else:
            print(f"[ERR] '{p_type}' pipeline HTTP {e.code}: {body[:300]}")


def main():
    print("=== OpenMetadata Ingestion Pipeline Setup ===")

    print("\n[1] Logging in...")
    token = login()
    print(f"    Token OK: {token[:25]}...")

    print("\n[2] Getting service 'iceberg-rest-catalog'...")
    try:
        svc_id = get_service_id(token)
        print(f"    Service ID: {svc_id}")
    except urllib.error.HTTPError as e:
        print(f"    [ERR] Service not found (HTTP {e.code}). Create service 'iceberg-rest-catalog' in OM UI first.")
        sys.exit(1)

    print("\n[3] Creating metadata pipeline...")
    create_pipeline(
        token,
        svc_id,
        "metadata",
        "iceberg-rest-catalog_metadata",
        {
            "type": "DatabaseMetadata",
            "markDeletedTables": True,
            "includeTables": True,
            "includeViews": True,
        },
    )

    print("\n[4] Creating dbt pipeline...")
    create_pipeline(
        token,
        svc_id,
        "dbt",
        "iceberg-rest-catalog_dbt",
        {
            "type": "DBT",
            "dbtConfigSource": {
                "dbtCatalogFilePath": "/opt/dbt/nessie_transform/target/catalog.json",
                "dbtManifestFilePath": "/opt/dbt/nessie_transform/target/manifest.json",
                "dbtRunResultsFilePath": "/opt/dbt/nessie_transform/target/run_results.json",
            },
        },
    )

    print("\n=== Done. Trigger DAG again in Airflow UI. ===")


if __name__ == "__main__":
    main()

