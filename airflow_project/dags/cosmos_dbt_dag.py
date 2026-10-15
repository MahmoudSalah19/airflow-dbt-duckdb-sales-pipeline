from datetime import datetime
import sys

from airflow.decorators import dag, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import TestBehavior, LoadMode

sys.path.append("/usr/local/airflow/include")

DBT_PROJECT_PATH = "/usr/local/airflow/include/dbt/dbt_project"
DBT_EXECUTABLE = "/usr/local/airflow/dbt_venv/bin/dbt"

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    manifest_path=f"{DBT_PROJECT_PATH}/target/manifest.json",
)

profile_config = ProfileConfig(
    profile_name="dbt_project",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml",
)

execution_config = ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE)


@dag(
    dag_id="bronze_dbt_cosmos",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    tags=["dbt", "duckdb", "cosmos"],
)
def pipeline():

    @task
    def load_bronze():
        from bronze import load_bronze as run
        run()

    silver = DbtTaskGroup(
        group_id="silver",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["path:models/silver", "path:tests/silver"],
            exclude=["path:tests/gold"],
            test_behavior=TestBehavior.AFTER_EACH,
        ),
    )

    gold = DbtTaskGroup(
        group_id="gold",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["path:models/gold", "path:tests/gold"],
            test_behavior=TestBehavior.AFTER_EACH,
        ),
    )

    load_bronze() >> silver >> gold


pipeline()