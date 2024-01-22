import requests
from typing import List


def delete_jobs(service_principal_token: str, api_url_job_delete: str, jobs_ids: List[str]) -> bool:
    """
    Delete jobs using their IDs.

    Parameters:
        service_principal_token (str): The service principal token for authorization in API calls.
        api_url_job_delete (str): The API endpoint for deleting jobs.
        jobs_ids (list): A list of job IDs to be deleted.

    Returns:
        bool: True if all jobs are successfully deleted, False otherwise.
    """

    success = True

    for job_id in jobs_ids:
        try:
            response = requests.post(
                api_url_job_delete,
                headers={
                    "Authorization": f"Bearer {service_principal_token}"
                },
                json={
                    "job_id": job_id
                }
            )
            if response.status_code != 200:
                print(f"Failed to delete job {job_id}, status code: {response.status_code}, response: {response.json()}")
                success = False
            else:
                print(f"Deleted job {job_id}.")
        except requests.exceptions.RequestException as e:
            print(f"Failed to delete job {job_id} due to an error: {e}")
            success = False
    
    return success
