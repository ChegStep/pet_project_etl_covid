from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

import os
from datetime import datetime

BASE_PATH = os.path.dirname(__file__)
DBT_PROJECT_PATH = os.path.join(BASE_PATH, "covid_dbt")
MANIFEST_PATH = os.path.join(DBT_PROJECT_PATH, "target", "manifest.json")

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_conn",
        profile_args={"schema": "raw"},
    ),
)

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        f"{os.environ['AIRFLOW_HOME']}/dags/covid_dbt",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/venv/bin/dbt"
    ),

    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="run_dbt_models",
    default_args={"retries": 2},
)