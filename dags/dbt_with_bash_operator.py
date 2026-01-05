from datetime import datetime
from pathlib import Path
import os
from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator
with DAG(
    dag_id="zdbt_with_bash_operator",
    # schedule_interval="0 0 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    project_path = Path(os.getcwd()).resolve() / 'my_dbt_mysql_project'
    project_dir = str(project_path)
    profiles_dir = str(project_path)

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"dbt seed --project-dir {project_dir} --profiles-dir {profiles_dir}"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {project_dir} --profiles-dir {profiles_dir}"
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"dbt snapshot --project-dir {project_dir} --profiles-dir {profiles_dir}"
    )

    dbt_seed >> dbt_run >> dbt_snapshot

 