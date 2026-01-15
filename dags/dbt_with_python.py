from datetime import datetime
from pathlib import Path
import os
from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator

from dbt.cli.main import dbtRunner, dbtRunnerResult
# import plugins.hello as hello
from hello import hi
from hello import add
from datacleaner import data_cleaner
# from dags.hello import hi

with DAG(
    dag_id="zdbt_with_python_runner",
    # schedule_interval="0 0 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    project_path = Path(os.getcwd()).resolve() / 'my_dbt_mysql_project'
    project_dir = str(project_path)
    profiles_dir = str(project_path)

    dbt = dbtRunner()

    cli_args = [["debug"], ["seed"], ["run"], ["snapshot"]]
    def run_dbt(arg):
        result: dbtRunnerResult = dbt.invoke(arg)
        print(result)
    
    for i, arg in enumerate(cli_args):
        arg += ["--project-dir", project_dir, "--profiles-dir", profiles_dir]
        task = PythonOperator(
            task_id=f"dbt_{arg[0]}",
            python_callable=run_dbt,
            op_args=[arg],
        )
        if i > 0:
            prev_task = dag.get_task(f"dbt_{cli_args[i-1][0]}")
            prev_task >> task

    hi_task = PythonOperator(
        task_id="hi_task",
        python_callable=hi
    )
    add_one = PythonOperator(
        task_id ="add_one",
        python_callable =add,
        op_args =[20]
    )
    prev_task = dag.get_task(f"dbt_{cli_args[-1][0]}")
    # prev_task
    prev_task >> [hi_task ,add_one]