import os

DATABRICKS_API_URL = "https://api.databricks.com"
CREATE_JOB_ENDPOINT = f"{DATABRICKS_API_URL}/api/2.0/jobs/create"
DELETE_JOB_ENDPOINT = f"{DATABRICKS_API_URL}/api/2.0/jobs/delete"
GET_JOB_ENDPOINT = f"{DATABRICKS_API_URL}/api/2.0/jobs/get"
UPDATE_JOB_ENDPOINT = f"{DATABRICKS_API_URL}/api/2.0/jobs/reset"

BUCKET_NAME = "databricks-workflow"

CURRENT_STATE_FILEPATH = "current_state.json"
HISTORIC_STATE_FILEPATH = "historic_state.json"

JOB_CONFIG_FILE_PATH = 'workflow/job_config.yaml'

SERVICE_PRINCIPAL_TOKEN = os.getenv("SERVICE_PRINCIPAL_TOKEN")