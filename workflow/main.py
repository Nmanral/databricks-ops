import logging
from typing import Optional, List
from datetime import datetime
from workflow.yaml_utils import read_config
from workflow.task_payload import create_task_payload
from workflow.json_utils import read_json_file_from_s3, write_json_to_s3
from workflow.update_jobs import update_jobs
from workflow.get_jobs import get_jobs
from workflow.delete_jobs import delete_jobs
from workflow.create_jobs import create_jobs
from workflow.version_utils import get_current_version, generate_version
from workflow.consts import (
    UPDATE_JOB_ENDPOINT,
    GET_JOB_ENDPOINT,
    DELETE_JOB_ENDPOINT,
    CREATE_JOB_ENDPOINT,
    BUCKET_NAME,
    CURRENT_STATE_FILEPATH,
    HISTORIC_STATE_FILEPATH,
    SERVICE_PRINCIPAL_TOKEN
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def update_extended_workflow_data_with_job_ids(
    extended_workflow_data: dict,
    workflow_data: dict,
    state_file_workflow_details: dict,
    existing_jobs_names: List[str],
    new_job_ids: Optional[List[int]] = None
) -> dict:
    """
    This function updates the extended_workflow_data with the job ids.

    Args:
        extended_workflow_data (dict): The extended workflow data.
        workflow_data (dict): The workflow data.
        state_file_workflow_details (dict): The state file workflow details.
        existing_jobs_names (List[str]): The list of existing job names.
        new_job_ids (Optional[List[int]], optional): The list of new job ids. Defaults to None.

    Returns:
        dict: The updated extended workflow data.
    """
    logging.info("Starting update_extended_workflow_data_with_job_ids function.")

    for workflow_name, workflow_details in workflow_data.items():
        job_name = workflow_details.get("job_name", "")
        logging.debug(f"Processing workflow: {workflow_name}, Job name: {job_name}")
        if job_name in existing_jobs_names:
            job_id = state_file_workflow_details.get(workflow_name, {}).get("job_id", None)
            extended_workflow_data[workflow_name]["job_id"] = job_id
            logging.info(f"Updated job ID for existing job '{job_name}': {job_id}")
        elif new_job_ids:
            job_id = new_job_ids.pop(0)
            extended_workflow_data[workflow_name]["job_id"] = job_id
            logging.info(f"Assigned new job ID {job_id} to workflow '{workflow_name}'")
        else:
            logging.warning(f"No job ID available for workflow '{workflow_name}'")

    logging.info("Completed update_extended_workflow_data_with_job_ids function.")
    return extended_workflow_data


def create_json_with_metadata(extended_workflow_data: dict, current_version: str):
    """
    This function creates the json with metadata.

    Args:
        extended_workflow_data (dict): The extended workflow data.
        current_version (str): The current version.

    Returns:
        dict: The JSON data with metadata.
    """
    logging.info("Starting to create JSON with metadata.")

    logging.debug(f"Current version: {current_version}")
    version = generate_version(current_version)
    logging.info(f"Generated new version: {version}")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.debug(f"Timestamp for JSON metadata: {timestamp}")

    metadata = {
        "version": version,
        "timestamp": timestamp
    }

    json_data = {
        "metadata": metadata,
        "config": extended_workflow_data
    }

    logging.info("JSON with metadata successfully created.")
    return json_data


def update_state_file(extended_workflow_data: dict):
    """
    This function updates the state file.

    Args:
        extended_workflow_data (dict): The extended workflow data.
    """
    logging.info("Starting the update of the state file.")
    current_version = get_current_version(BUCKET_NAME, CURRENT_STATE_FILEPATH)
    logging.info(f"Retrieved current version: {current_version}")
    json_data = create_json_with_metadata(extended_workflow_data, current_version)
    version_name = json_data.get("metadata", {}).get("version", "")
    logging.info(f"Created new JSON data with version: {version_name}")

    write_json_to_s3(json_data, BUCKET_NAME, f"{HISTORIC_STATE_FILEPATH}/state_file_{version_name}.json")
    logging.info(f"Written new state file to historic path: {HISTORIC_STATE_FILEPATH}/state_file_{version_name}.json")
    write_json_to_s3(json_data, BUCKET_NAME, CURRENT_STATE_FILEPATH)
    logging.info(f"Updated current state file at path: {CURRENT_STATE_FILEPATH}")

    logging.info("State file update completed.")


def identify_removed_jobs(extended_workflow_data: dict, state_file_workflow_details: dict):
    """
    This function identifies the removed jobs.

    Args:
        extended_workflow_data (dict): The extended workflow data.
        state_file_workflow_details (dict): The state file workflow details.

    Returns:
        List: The list of removed job ids.
    """
    logging.info("Starting to identify removed jobs.")
    removed_jobs_ids = []
    for workflow_name, workflow_details in state_file_workflow_details.items():
        if workflow_name not in extended_workflow_data:
            removed_jobs_ids.append(workflow_details.get("job_id", None))
            logging.info(f"Job removed: {workflow_name} with job ID {workflow_details.get('job_id', None)}")
        else:
            logging.debug(f"Job retained: {workflow_name}")
    logging.info(f"Removed jobs identified: {removed_jobs_ids}")
    return removed_jobs_ids


def process_workflow(yaml_filepath: str) -> None:
    """
    This function is the entry point for the workflow.
    It reads the yaml file, creates the payload for the jobs and
    creates the jobs.

    Args:
        yaml_filepath (str): The path to the YAML file.
    Returns:
        dict: The extended workflow data.
        dict: The workflow data.
        dict: The state file workflow details.
    """
    extended_workflow_data = {}

    state_file_workflow_details = (
        read_json_file_from_s3(bucket_name=BUCKET_NAME, s3_key=CURRENT_STATE_FILEPATH)
        .get("config", {})
    )
    logging.debug("State file workflow details read from S3.")

    workflow_data = {k: v for k, v in read_config(yaml_filepath).items() if k.startswith('workflow')}
    logging.info(f"Workflow data read from YAML file: {len(workflow_data)} workflows found.")


    for workflow_name, workflow_details in workflow_data.items():
        logging.info(f"Processing workflow: {workflow_name}")

        tasks_payload = []
        email_on_success = []
        email_on_failure = []

        for task_configurations in workflow_details.get("tasks", []):
            if 'gcp_connection' in task_configurations:
                gcp_connection_required = task_configurations['gcp_connection']
                task_payload = create_task_payload(task_configurations, gcp_connection_required)
            else:
                task_payload = create_task_payload(task_configurations)

            tasks_payload.append(task_payload)
            email_on_success.extend(task_configurations.get("email_on_success", []))
            email_on_failure.extend(task_configurations.get("email_on_failure", []))

        data = {
            "name": workflow_details.get("job_name", ""),
            "email_notifications": {
                "on_success": email_on_success,
                "on_failure": email_on_failure
            },
            "webhook_notifications": {},
            "timeout_seconds": 0,
            "schedule": {
                "quartz_cron_expression": workflow_details.get("schedule", ""),
                "timezon_id": "Europe/London",
                "pause_status": "UNPAUSED"
            },
            "max_concurrent_runs": 1,
            "tasks": tasks_payload,
            "format": "MULTI_TASK",
            "access_control_list": [
                {
                    "group_name": "Uk",
                    "permission_level": "CAN_MANAGE"
                }
            ]
        }

        extended_workflow_data[workflow_name] = data
        logging.debug(f"Workflow {workflow_name} processed.")

    logging.info("All workflows processed.")
    return extended_workflow_data, workflow_data, state_file_workflow_details


def update_jobs_workflow(
    extended_workflow_data: dict,
    workflow_data: dict,
    state_file_workflow_details: dict,
    existing_jobs_names: List[str]
):
    """
    This function updates the jobs workflow.

    Args:
        extended_workflow_data (dict): The extended workflow data.
        workflow_data (dict): The workflow data.
        state_file_workflow_details (dict): The state file workflow details.
        existing_jobs_names (List[str]): The list of existing job names.
    """
    jobs_to_update = {}
    for name, details in extended_workflow_data.items():

        current_sate_data = state_file_workflow_details.get(name, {}).copy()

        # Compare the extended_workflow_data with the historic workflow_data
        if details == current_sate_data:
            logging.debug(f"No update required for job '{name}' (data unchanged).")
            continue
        if details != current_sate_data:
            jobs_to_update[name] = (current_sate_data.pop("job_id", None), details)
            logging.info(f"Job '{name}' marked for update.")


    if jobs_to_update:
        logging.info("Preparing to update jobs.")
        job_ids_to_update = [job_id for job_id, _ in jobs_to_update.values()]
        job_data_to_update = [job_data for _, job_data in jobs_to_update.values()]

        update_status = update_jobs(SERVICE_PRINCIPAL_TOKEN, UPDATE_JOB_ENDPOINT, job_ids_to_update, job_data_to_update)
        logging.info(f"Jobs update status: {update_status}")

        if update_status:
            update_state_file(
                update_extended_workflow_data_with_job_ids(
                    extended_workflow_data,
                    workflow_data,
                    state_file_workflow_details,
                    existing_jobs_names
                )
            )
            logging.info("State file updated with new job IDs.")
        else:
            logging.error("Failed to update jobs. State file not updated.")
    else:
        logging.info("No jobs require updating.")

    logging.info("Jobs workflow update process completed.")


def delete_jobs_workflow(
    extended_workflow_data: dict,
    workflow_data: dict,
    state_file_workflow_details: dict,
    existing_jobs_names: List[str]
):
    """
    This function performs the delete jobs workflow.

    Args:
        extended_workflow_data (dict): The extended workflow data.
        workflow_data (dict): The workflow data.
        state_file_workflow_details (dict): The state file workflow details.
        existing_jobs_names (List[str]): The list of existing job names.
    """
    jobs_to_delete = identify_removed_jobs(extended_workflow_data, state_file_workflow_details)
    if jobs_to_delete:
        logging.info(f"Jobs identified for deletion: {jobs_to_delete}")
        delete_status = delete_jobs(SERVICE_PRINCIPAL_TOKEN, DELETE_JOB_ENDPOINT, jobs_to_delete)
        if delete_status:
            logging.info("Jobs successfully deleted. Updating state file.")
            update_state_file(
                update_extended_workflow_data_with_job_ids(
                    extended_workflow_data,
                    workflow_data,
                    state_file_workflow_details,
                    existing_jobs_names
                )
            )
            logging.info("State file updated after deleting jobs.")
        else:
            logging.error("Failed to delete jobs. State file not updated.")
    else:
        logging.info("No jobs identified for deletion.")

    logging.info("Delete jobs workflow process completed.")


def create_jobs_workflow(
    extended_workflow_data: dict,
    workflow_data: dict,
    state_file_workflow_details: dict,
    existing_jobs_names: List[str]
):
    """
    This function performs the create jobs workflow.

    Args:
        extended_workflow_data (dict): The extended workflow data.
        workflow_data (dict): The workflow data.
        state_file_workflow_details (dict): The state file workflow details.
        existing_jobs_names (List[str]): The list of existing job names.
    """
    if existing_jobs_names:
        new_job_ids = create_jobs(existing_jobs_names, SERVICE_PRINCIPAL_TOKEN, CREATE_JOB_ENDPOINT, list(extended_workflow_data.values()))
        if new_job_ids:
            logging.info(f"New jobs created with IDs: {new_job_ids}")
            update_state_file(
                update_extended_workflow_data_with_job_ids(
                    extended_workflow_data,
                    workflow_data,
                    state_file_workflow_details,
                    existing_jobs_names,
                    new_job_ids
                )
            )
            logging.info("State file updated with new job IDs.")
        else:
            logging.warning("No new job IDs returned. State file not updated.")
    else:
        logging.info("No new job names provided; no new jobs created.")

    logging.info("Job creation process in the workflow completed.")


def run_workflow(yaml_filepath: str) -> None:
    """
    This function runs the workflow.

    Args:
        yaml_filepath (str): The path to the YAML file.
    """
    logging.info(f"Starting workflow run with YAML file: {yaml_filepath}")

    logging.info("Processing workflow.")
    extended_workflow_data, workflow_data, state_file_workflow_details = process_workflow(yaml_filepath)
    logging.info("Workflow processing completed.")

    logging.info("Retrieving existing jobs.")
    existing_jobs_names = get_jobs(SERVICE_PRINCIPAL_TOKEN, GET_JOB_ENDPOINT)
    logging.info(f"Existing jobs retrieved: {existing_jobs_names}")

    logging.info("Checking workflows for updates.")
    update_jobs_workflow(extended_workflow_data, workflow_data, state_file_workflow_details, existing_jobs_names)

    logging.info("Checking workflows for deletions.")
    delete_jobs_workflow(extended_workflow_data, workflow_data, state_file_workflow_details, existing_jobs_names)

    logging.info("Cheking workflows for creation.")
    create_jobs_workflow(extended_workflow_data, workflow_data, state_file_workflow_details, existing_jobs_names)

    logging.info("Workflow run completed.")
