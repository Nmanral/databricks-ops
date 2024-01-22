import requests
from typing import List, Dict


def create_jobs(existing_jobs: List[str], service_principal_token: str, api_url_job_create: str, jobs_data: List[Dict]) -> List[str]:
    """
    Create a new Jobs in batch using the provided jobs data

    Parameters:
        existing_jobs (list): A list of existing job names.
        service_principal_token (str): The service principal token for authorization in API calls.
        api_url_job_create (str): The API endpoint for creating jobs.
        jobs_data (list): A list of dictionaries, each representing the data for a new job.

    Returns:
        list: A list of job IDs for the newly created jobs.
    """

    new_job_ids = []
    for data in jobs_data:
        job_name = data['name']
        if job_name not in existing_jobs:
            try:
                response = requests.post(
                    api_url_job_create,
                    headers={
                        "Authorization": f"Bearer {service_principal_token}"
                    },
                    json=data
                )
                if response.status_code == 200:
                    new_job_id = response.json().get('job_id')
                    if new_job_id:
                        new_job_ids.append(new_job_id)
                        print(f"created job {job_name} with ID {new_job_id}.")
                    else:
                        print(f"failed to create job {job_name}, no job ID returned.")
                else:
                    print(f"Failed to create job {job_name}, status code: {response.status_code}, response: {response.json()}")

            except requests.exceptions.RequestException as e:
                print(f"failed to create job {job_name} due to an error: {e}")
        else:
            print(f"Job name {job_name} is missing or already exists.")

    return new_job_ids
