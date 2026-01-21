from airflow.models.dag import DAG
from datetime import datetime, timedelta
# from airflow.operators import DataTransferOperator
from demo_plugin import DataTransferOperator, FileCountSensor, MySQLToPostgresHook
from airflow.providers.standard.operators.python import PythonOperator


# dag = DAG('plugins_dag', schedule=timedelta(1), start_date=datetime(2026, 1, 20), catchup=False)

# t1 = DataTransferOperator(
#         task_id='data_transfer',
#         source_file_path = '/opt/airflow/plugins/source.txt',
#         dest_file_path='/opt/airflow/plugins/destination.txt',
#         delete_list = ['Airflow', 'is'],
#         dag=dag
#     )

# dag = DAG('plugins_dag', schedule=timedelta(1), start_date=datetime(2026, 1, 20), catchup=False)

# t1 = FileCountSensor(
#     task_id = 'file_count_sensor',
#     dir_path = '/opt/airflow/plugins/',
#     conn_id = 'fs_default',
#     poke_interval = 5,
#     timeout = 100,
#     dag = dag
# )
 
dag = DAG('plugins_dag', schedule=timedelta(1), start_date=datetime(2026, 1, 20), catchup=False)

def trigger_hook():
    MySQLToPostgresHook().copy_table('mysql_airflow_conn_test', 'postgres_conn_test')
    print("done")

t1 = PythonOperator(
    task_id = 'mysql_to_postgres',
    python_callable = trigger_hook,
    dag = dag
)

