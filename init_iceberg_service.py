import requests
import json
import time
import sys

OPENMETADATA_URL = "http://openmetadata:8585/api"
MAX_RETRIES = 30
RETRY_DELAY = 10
AUTH_TOKEN = None

def wait_for_openmetadata():
    for i in range(MAX_RETRIES):
        try:
            response = requests.get(f"{OPENMETADATA_URL}/v1/system/version", timeout=5)
            if response.status_code == 200:
                print(f"OpenMetadata is ready! Version: {response.json()}")
                return True
        except Exception as e:
            print(f"Waiting for OpenMetadata... ({i+1}/{MAX_RETRIES}): {str(e)}")
        time.sleep(RETRY_DELAY)
    return False

def get_auth_token():
    global AUTH_TOKEN

    try:
        token_response = requests.get(
            f"{OPENMETADATA_URL}/v1/users/auth-mechanism/ingestion-bot",
            timeout=10
        )

        if token_response.status_code == 200:
            auth_data = token_response.json()
            AUTH_TOKEN = auth_data.get("config", {}).get("JWTToken")
            if AUTH_TOKEN:
                print("Successfully retrieved auth token from ingestion-bot")
                return AUTH_TOKEN

        print("No authentication token found, will try without auth")
        return None

    except Exception as e:
        print(f"Error getting auth token: {str(e)}")
        print("Will proceed without authentication")
        return None

def get_headers():
    headers = {"Content-Type": "application/json"}
    if AUTH_TOKEN:
        headers["Authorization"] = f"Bearer {AUTH_TOKEN}"
    return headers

def create_iceberg_service():
    service_config = {
        "name": "iceberg-rest-catalog",
        "serviceType": "Iceberg",
        "description": "Iceberg REST Catalog Service",
        "connection": {
            "config": {
                "type": "Iceberg",
                "catalog": {
                    "name": "rest",
                    "uri": "http://rest:8181",
                    "warehouse": "s3://warehouse/"
                },
                "fileSystem": {
                    "securityConfig": {
                        "awsAccessKeyId": "admin",
                        "awsSecretAccessKey": "password",
                        "awsRegion": "us-east-1"
                    },
                    "endPointURL": "http://minio:9000"
                }
            }
        }
    }

    try:
        check_response = requests.get(
            f"{OPENMETADATA_URL}/v1/services/databaseServices/name/iceberg-rest-catalog",
            headers=get_headers(),
            timeout=10
        )

        if check_response.status_code == 200:
            print("Iceberg service already exists!")
            return check_response.json()

        response = requests.post(
            f"{OPENMETADATA_URL}/v1/services/databaseServices",
            json=service_config,
            headers=get_headers(),
            timeout=30
        )

        if response.status_code in [200, 201]:
            print("Successfully created Iceberg service!")
            return response.json()
        else:
            print(f"Failed to create service: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        print(f"Error creating Iceberg service: {str(e)}")
        return None

def create_ingestion_pipeline(service_id):
    pipeline_config = {
        "name": "iceberg-metadata-ingestion",
        "displayName": "Iceberg Metadata Ingestion",
        "pipelineType": "metadata",
        "service": {
            "id": service_id,
            "type": "databaseService"
        },
        "sourceConfig": {
            "config": {
                "type": "DatabaseMetadata",
                "markDeletedTables": True,
                "includeTables": True,
                "includeViews": True,
                "databaseFilterPattern": {
                    "includes": [".*"]
                },
                "schemaFilterPattern": {
                    "includes": [".*"]
                },
                "tableFilterPattern": {
                    "includes": [".*"]
                }
            }
        },
        "airflowConfig": {
            "scheduleInterval": "0 * * * *"
        }
    }

    try:
        response = requests.post(
            f"{OPENMETADATA_URL}/v1/services/ingestionPipelines",
            json=pipeline_config,
            headers=get_headers(),
            timeout=30
        )

        if response.status_code in [200, 201]:
            print("Successfully created ingestion pipeline!")
            return response.json()
        else:
            print(f"Failed to create pipeline: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        print(f"Error creating ingestion pipeline: {str(e)}")
        return None

def trigger_ingestion_pipeline(pipeline_id):
    try:
        response = requests.post(
            f"{OPENMETADATA_URL}/v1/services/ingestionPipelines/trigger/{pipeline_id}",
            headers=get_headers(),
            timeout=30
        )

        if response.status_code in [200, 201]:
            print("Successfully triggered ingestion pipeline!")
            return True
        else:
            print(f"Failed to trigger pipeline: {response.status_code} - {response.text}")
            return False

    except Exception as e:
        print(f"Error triggering pipeline: {str(e)}")
        return False

def main():
    print("Starting Iceberg service initialization...")

    if not wait_for_openmetadata():
        print("OpenMetadata is not available after maximum retries!")
        sys.exit(1)

    get_auth_token()

    service = create_iceberg_service()
    if not service:
        print("Failed to create Iceberg service!")
        sys.exit(1)

    service_id = service.get("id")
    print(f"Service ID: {service_id}")

    pipeline = create_ingestion_pipeline(service_id)
    if not pipeline:
        print("Failed to create ingestion pipeline!")
        sys.exit(1)

    pipeline_id = pipeline.get("id")
    print(f"Pipeline ID: {pipeline_id}")

    time.sleep(5)

    if trigger_ingestion_pipeline(pipeline_id):
        print("Iceberg service setup completed successfully!")
    else:
        print("Service created but failed to trigger initial ingestion")

if __name__ == "__main__":
    main()
