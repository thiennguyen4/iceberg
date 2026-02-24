import requests
import json
import time
import sys
import os

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
        print("Logging in with admin credentials to get JWT token...")

        login_payload = {
            "email": "admin@open-metadata.org",
            "password": "YWRtaW4="
        }

        login_response = requests.post(
            f"{OPENMETADATA_URL}/v1/users/login",
            json=login_payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )

        if login_response.status_code == 200:
            login_data = login_response.json()
            AUTH_TOKEN = login_data.get("accessToken")
            if AUTH_TOKEN:
                print(f"Successfully logged in and retrieved JWT token (length: {len(AUTH_TOKEN)})")
                return AUTH_TOKEN
            else:
                print(f"Login successful but no accessToken in response: {login_data}")
        else:
            print(f"Login failed: {login_response.status_code}")
            print(f"Response: {login_response.text}")

        print("Trying to get token from ingestion-bot...")
        token_response = requests.get(
            f"{OPENMETADATA_URL}/v1/users/auth-mechanism/ingestion-bot",
            timeout=10
        )
import requests
import json
import time
import sys
import os

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
        print("Logging in with admin credentials to get JWT token...")

        login_payload = {
            "email": "admin@open-metadata.org",
            "password": "YWRtaW4="
        }

        login_response = requests.post(
            f"{OPENMETADATA_URL}/v1/users/login",
            json=login_payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )

        if login_response.status_code == 200:
            login_data = login_response.json()
            AUTH_TOKEN = login_data.get("accessToken")
            if AUTH_TOKEN:
                print(f"Successfully logged in and retrieved JWT token (length: {len(AUTH_TOKEN)})")
                return AUTH_TOKEN
            else:
                print(f"Login successful but no accessToken in response: {login_data}")
        else:
            print(f"Login failed: {login_response.status_code}")
            print(f"Response: {login_response.text}")

        print("Trying to get token from ingestion-bot...")
        token_response = requests.get(
            f"{OPENMETADATA_URL}/v1/users/auth-mechanism/ingestion-bot",
            timeout=10
        )

        if token_response.status_code == 200:
            auth_data = token_response.json()
            AUTH_TOKEN = auth_data.get("config", {}).get("JWTToken")
            if AUTH_TOKEN:
                print(f"Successfully retrieved JWT token from ingestion-bot (length: {len(AUTH_TOKEN)})")
                return AUTH_TOKEN

        env_token = os.environ.get("OPENMETADATA_JWT_TOKEN")
        if env_token:
            print(f"Fallback to environment token (length: {len(env_token)})")
            AUTH_TOKEN = env_token
            return AUTH_TOKEN

        print("WARNING: No JWT token available!")
        return None

    except Exception as e:
        print(f"Error getting auth token: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def get_headers():
    headers = {"Content-Type": "application/json"}
    if AUTH_TOKEN:
        headers["Authorization"] = f"Bearer {AUTH_TOKEN}"
    return headers

def create_iceberg_service():
    print("Creating Iceberg Service...")
    service_config = {
        "name": "iceberg-rest-catalog",
        "displayName": "Iceberg REST (MinIO)",
        "serviceType": "Iceberg",
        "description": "Iceberg Catalog connected via REST and MinIO S3",
        "connection": {
            "config": {
                "type": "Iceberg",
                "catalog": {
                    "name": "iceberg",
                    "connection": {
                        "uri": "http://rest:8181",
                        "credential": {
                            "clientId": "dummy"
                        },
                        "fileSystem": {
                            "s3": {
                                "awsAccessKeyId": "admin",
                                "awsSecretAccessKey": "password",
                                "awsRegion": "us-east-1",
                                "endPointURL": "http://minio:9000" # Cần dòng này để trỏ về MinIO
                            }
                        }
                    }
                },

            }
        }
    }

    try:
        check = requests.get(
            f"{OPENMETADATA_URL}/v1/services/databaseServices/name/iceberg-rest-catalog",
            headers=get_headers()
        )
        if check.status_code == 200:
            print("Service already exists. Skipping creation.")
            return check.json()

        response = requests.post(
            f"{OPENMETADATA_URL}/v1/services/databaseServices",
            json=service_config,
            headers=get_headers()
        )
        if response.status_code in [200, 201]:
            print("Service created successfully.")
            return response.json()
        else:
            print(f"Error creating service: {response.text}")
            return None
    except Exception as e:
        print(f"Exception: {e}")
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

        if token_response.status_code == 200:
            auth_data = token_response.json()
            AUTH_TOKEN = auth_data.get("config", {}).get("JWTToken")
            if AUTH_TOKEN:
                print(f"Successfully retrieved JWT token from ingestion-bot (length: {len(AUTH_TOKEN)})")
                return AUTH_TOKEN

        env_token = os.environ.get("OPENMETADATA_JWT_TOKEN")
        if env_token:
            print(f"Fallback to environment token (length: {len(env_token)})")
            AUTH_TOKEN = env_token
            return AUTH_TOKEN

        print("WARNING: No JWT token available!")
        return None

    except Exception as e:
        print(f"Error getting auth token: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def get_headers():
    headers = {"Content-Type": "application/json"}
    if AUTH_TOKEN:
        headers["Authorization"] = f"Bearer {AUTH_TOKEN}"
    return headers

def create_iceberg_service():
    print("Creating Iceberg Service...")
    service_config = {
        "name": "iceberg-rest-catalog",
        "displayName": "Iceberg REST (MinIO)",
        "serviceType": "Iceberg",
        "description": "Iceberg Catalog connected via REST and MinIO S3",
        "connection": {
            "config": {
                "type": "Iceberg",
                "catalog": {
                    "name": "iceberg",
                    "connection": {
                        "uri": "http://rest:8181",
                        "credential": {
                            "clientId": "dummy"
                        },
                        "fileSystem": {
                            "s3": {
                                "awsAccessKeyId": "admin",
                                "awsSecretAccessKey": "password",
                                "awsRegion": "us-east-1",
                                "endPointURL": "http://minio:9000" # Cần dòng này để trỏ về MinIO
                            }
                        }
                    }
                },

            }
        }
    }

    try:
        check = requests.get(
            f"{OPENMETADATA_URL}/v1/services/databaseServices/name/iceberg-rest-catalog",
            headers=get_headers()
        )
        if check.status_code == 200:
            print("Service already exists. Skipping creation.")
            return check.json()

        response = requests.post(
            f"{OPENMETADATA_URL}/v1/services/databaseServices",
            json=service_config,
            headers=get_headers()
        )
        if response.status_code in [200, 201]:
            print("Service created successfully.")
            return response.json()
        else:
            print(f"Error creating service: {response.text}")
            return None
    except Exception as e:
        print(f"Exception: {e}")
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
