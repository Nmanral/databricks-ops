import json
from enum import Enum
from typing import Dict, Optional


class TaskType(Enum):
    NOTEBOOK = "NOTEBOOK"
    PYTHON = "PYTHON"
    SQL = "SQL"
    DBT = "DBT"


def add_gcp_spark_conf(cluster_config: Dict, gcp_connection_required: Dict):
    """
    Adds GCP specific Spark configurations to the cluster configuration.

    Args:
        cluster_config (Dict): The cluster configuration to which the Spark settings will be added.
        gcp_connection_required (Dict): A dictionary containing the GCP connection details such as service account email,
            project ID, and private keys.

    Raises:
        KeyError: If required GCP connection details are missing in gcp_connection_required dictionary.
    """
    spark_conf = {
        "spark.hadoop.google.cloud.auth.service.account.enable": "true",
        "spark.hadoop.fs.gs.auth.service.account.email": gcp_connection_required.get("service_account_email"),
        "spark.hadoop.fs.gs.project.id": gcp_connection_required.get("project_id"),
        "spark.hadoop.fs.gs.auth.service.account.private.key": f"{{{{secrets/scope/{gcp_connection_required.get('service_account_private_key')}}}}}",
        "spark.hadoop.fs.gs.auth.service.account.private.key.id": f"{{{{secrets/scope/{gcp_connection_required.get('service_account_private_id')}}}}}"
    }

    cluster_config["spark_conf"].update(spark_conf)


def build_notebook_payload(task_config: Dict, cluster_config: Dict, gcp_connection_required: Optional[Dict] = None) -> Dict:
    """
    Builds the payload for a notebook task.

    Args:
        task_config (Dict): The configuration for the notebook task.
        cluster_config (Dict): The cluster configuration.
        gcp_connection_required (Optional[Dict]): Optional GCP connection details for Spark configuration.

    Returns:
        Dict: A dictionary representing the notebook task payload.

    Raises:
        ValueError: If the 'filepath' key is missing in the task configuration.
    """
    if not task_config.get("filepath"):
        raise ValueError("Notebook task requires filepath key in the task configuration.")

    if gcp_connection_required:
        add_gcp_spark_conf(cluster_config, gcp_connection_required)

    return {
        "notebook_task": {
            "notebook_path": task_config["filepath"],
            "source": "GIT"
        }
    }


def build_python_payload(task_config: Dict, cluster_config: Dict, gcp_connection_required: Optional[Dict] = None) -> Dict:
    """
    Builds the payload for a Python task.

    Args:
        task_config (Dict): The configuration for the Python task.
        cluster_config (Dict): The cluster configuration.
        gcp_connection_required (Optional[Dict]): Optional GCP connection details for Spark configuration.

    Returns:
        Dict: A dictionary representing the Python task payload.

    Raises:
        ValueError: If the 'filepath' key is missing in the task configuration.
    """
    if not task_config.get("filepath"):
        raise ValueError("Python task requires filepath key in the task configuration.")
    if gcp_connection_required:
        add_gcp_spark_conf(cluster_config, gcp_connection_required)

    return {
        "spark_python_task": {
            "python_file": task_config["filepath"],
            "parameters": task_config.get("parameters", []) or [],
            "source": "GIT"
        }
    }


def build_sql_payload(task_config: Dict) -> Dict:
    """
    Builds the payload for an SQL task.

    Args:
        task_config (Dict): The configuration for the SQL task.

    Returns:
        Dict: A dictionary representing the SQL task payload.

    Raises:
        ValueError: If the 'filepath' key is missing in the task configuration.
    """
    if not task_config.get("filepath"):
        raise ValueError("SQL task requires filepath key in the task configuration.")

    return {
        "sql_task": {
            "file": {
                "path": task_config["sql_file_path"]
            },
            "warehoouse_id": "warehouse_id"
        }
    }


def build_dbt_payload(task_config: Dict) -> Dict:
    """
    Builds the payload for a DBT task.

    Args:
        task_config (Dict): The configuration for the DBT task.

    Returns:
        Dict: A dictionary representing the DBT task payload.

    Raises:
        ValueError: If the 'filepath' key is missing in the task configuration.
    """
    if not task_config.get("filepath"):
        raise ValueError("DBT task requires filepath key in the task configuration.")

    return {
        "dbt_task": {
            "project_directory": task_config["filepath"],
            "commands": task_config.get("commands", []),
            "warehouse_id": "warehouse_id"
        },
        "libraries": [
            {
                "pypi": {
                    "package": "dbt-databricks>=1.0.0,<2.0.0"
                }
            }
        ]
    }


def create_task_payload(task_config: Dict, gcp_connection_required: Optional[Dict] = None) -> Dict:
    """
    Creates the task payload based on the task configuration and optional GCP connection.

    Args:
        task_config (Dict): The configuration for the task.
        gcp_connection_required (Optional[Dict]): Optional GCP connection details for Spark configuration.

    Returns:
        Dict: A dictionary representing the task payload.

    Raises:
        ValueError: If the task configuration is missing essential keys like 'task_name' or if there is an error in loading
                    the cluster configuration.
    """
    if not task_config.get("task_name"):
        raise ValueError("Task Configuration must have a name key.")

    cluster_config = {}
    cluster_config_path = task_config.get("cluster_config_path", None)

    try:
        if cluster_config_path:
            with open(cluster_config_path, "r") as config_file:
                cluster_config = json.load(config_file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        raise ValueError(f"Error loading cluster config: {e}")

    task_type = TaskType(task_config["tasktype"])

    payload_builder = {
        TaskType.NOTEBOOK: build_notebook_payload,
        TaskType.PYTHON: build_notebook_payload,
        TaskType.SQL: build_sql_payload,
        TaskType.DBT: build_dbt_payload
    }

    if task_type not in payload_builder:
        raise ValueError(f"Unsupported task type: {task_config.get('tasktype')}")

    payload = payload_builder[task_type](task_config, cluster_config, gcp_connection_required)

    common_payload = {
        "task_key": task_config["task_name"],
        "new_cluster": cluster_config,
        "libraries": [],
        "timeout_seconds": task_config.get("timeout_seconds", 0),
        "email_notifications": {
            "on_success": task_config.get("email_on_success", []) or [],
            "on_failure": task_config.get("email_on_failure", []) or []
        }
    }

    if "libraries" in task_config:
        common_payload["libraries"].extend(task_config["libraries"])

    payload.update(common_payload)

    if task_config.get("depends_on"):
        payload["depends_on"] = [{"task_key": task_config["depends_on"]}]

    return payload