import requests
from typing import List, Optional


def get_jobs(service_principal_token: str, api_url_job_list: str) -> Optional[List[str]]:
    """
    Retrieve a list of all existing job names.

    Parameters:
        service_principal_token (str): The service principal token for authorization in API calls.
        api_url_job_list (str): The API endpoint for deleting jobs.
    Returns:
        list: A list of all existing job names, if an error occurs during API call, it returns an empty string.
    """

    page_token = None
    all_job_names = []

    while True:
        url = f"{api_url_job_list}?page_token={page_token}" if page_token else api_url_job_list

        try:
            response = requests.get(
                url,
                headers={
                    "Authorization": f"Bearer {service_principal_token}"
                }
            )
            if response.status_code == 200:
                data = response.json()
                jobs = data.get('jobs', [])
                all_job_names.extend(job.get('settings', {}).get('name', 'unknown job name') for job in jobs)

                page_token = data.get('next_page_token', None)
                if not page_token:
                    break
            else:
                print(f"Failed to list jobs. Status code: {response.status_code}, response: {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Failed to list jobs due to an error: {e}")
            return None
    
    return all_job_names
