import requests
from typing import List, Dict


def update_jobs(service_principal_token: str, api_url_job_update: str, job_ids: List[str], jobs_update_data: List[Dict]) -> bool:
    """
    Create a new Jobs in batch using the provided jobs data

    Parameters:
        service_principal_token (str): The service principal token for authorization in API calls.
        api_url_job_update (str): The API endpoint for updating jobs.
        jobs_ids (list): A list of job IDs to be updated.
        jobs_update_data (list): A list of dictionary, each representing the data for updating jobs.

    Returns:
        bool: True if all updates were successful, False otherwise.
    """

    success = True

    for job_id, job_data in zip(job_ids, jobs_update_data):
        json_data = {
            "job_id": job_id,
            "new_settings": job_data
        }

        try:
            response = requests.post(
                api_url_job_update,
                headers={
                    "Authorization": f"Bearer {service_principal_token}"
                },
                json=json_data
            )

            if response.status_code == 200:
                print(f"Updated job with ID {job_id}.")
            else:
                print(f"Failed to update job with ID {job_id}.")
                success = False
        except requests.exceptions.RequestException as e:
            print(f"Failed to update job with ID {job_id} due to an error: {e}")
            success = False

    return success
