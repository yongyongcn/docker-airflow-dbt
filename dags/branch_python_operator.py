import airflow
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


args = {
    'owner': 'Airflow',
    'start_date': datetime.strftime(datetime.now() ,'%Y-%m-%d'),
}

def push_function(**kwargs):
    pushed_value=4
    ti = kwargs['ti']
    ti.xcom_push(key="pushed_value", value=pushed_value)

def branch_function(**kwargs):
    ti = kwargs['ti']
    pulled_value = ti.xcom_pull(key='pushed_value', task_ids='push_task')
    if pulled_value %2 == 0:
        return 'even_task'
    else:
        return 'odd_task'


with DAG(dag_id='zbranching', default_args=args, schedule="@daily") as dag:

    push_task = PythonOperator(task_id='push_task', python_callable=push_function )

    branch_task = BranchPythonOperator(task_id='branch_task', python_callable=branch_function)

    even_task = BashOperator(task_id='even_task', bash_command='echo "Got an even value."')

    odd_task = BashOperator(task_id='odd_task', bash_command='echo "Got an odd value."')

    push_task >> branch_task >>[even_task, odd_task]
