from airflow.operators.empty import EmptyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


with DAG(dag_id='zlatest_only_example', schedule=timedelta(hours=6), start_date=datetime(2026, 1, 15), catchup=True) as dag:

    t1 = LatestOnlyOperator(task_id = 'latest_only')

    t2 = EmptyOperator(task_id='task2')

    t3 = EmptyOperator(task_id='task3')

    t4 = EmptyOperator(task_id='task4')

    t5 = EmptyOperator(task_id='task5')

    t1 >> [t2, t4, t5]
