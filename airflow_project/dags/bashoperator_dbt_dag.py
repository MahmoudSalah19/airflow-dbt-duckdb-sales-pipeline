from datetime import datetime
import sys

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

# bronze.py lives in include/, outside dags/, so the scheduler doesn't parse it as a DAG
sys.path.append("/usr/local/airflow/include")

DBT_DIR = "/usr/local/airflow/include/dbt/dbt_project"
DBT = "/usr/local/airflow/dbt_venv/bin/dbt"
DBT_ARGS = "--profiles-dir . --target dev"


def dbt_cmd(command: str, select: str, exclude: str | None = None) -> str:
    cmd = f"cd {DBT_DIR} && {DBT} {command} --select {select}"
    if exclude:
        cmd += f" --exclude {exclude}"
    return f"{cmd} {DBT_ARGS}"


@dag(
    dag_id="bronze_dbt_bashoperator",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # manual trigger; change to "@daily" later
    catchup=False,
    max_active_tasks=1,     # DuckDB allows a single writer
    tags=["dbt", "duckdb"],
)
def pipeline():

    @task
    def load_bronze():
        from bronze import load_bronze as run
        run()

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=dbt_cmd("run", "path:models/silver"),
    )

    # exclude gold singular tests: they ref silver models, so dbt would
    # otherwise pick them up here before the gold layer exists
    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=dbt_cmd(
            "test",
            "path:models/silver path:tests/silver",
            exclude="path:tests/gold",
        ),
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=dbt_cmd("run", "path:models/gold"),
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=dbt_cmd("test", "path:models/gold path:tests/gold"),
    )

    (
        load_bronze()
        >> dbt_run_silver
        >> dbt_test_silver
        >> dbt_run_gold
        >> dbt_test_gold
    )


pipeline()