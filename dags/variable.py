from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 11, 27),
}

dag = DAG("variable", default_args=default_args )

t1 = BashOperator(task_id="print_path", bash_command="echo {{var.value.source_path}}", dag=dag)
