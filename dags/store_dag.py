from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datacleaner import data_cleaner
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.smtp.operators.smtp import EmailOperator

ds = datetime.strftime(datetime.now() ,'%Y-%m-%d')

MYSQL_CONN_ID = "mysql_airflow_conn_test"
DATABASE_NAME = "dbt_test_airflow"
default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2026,1,12),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}
dag = DAG('store_dag', default_args = default_args, template_searchpath=['/opt/airflow/data/sql_files'],  catchup= False)

t1 = BashOperator(task_id ='check_file_exists', bash_command ='shasum /opt/airflow/data/csv_files/raw_store_transactions.csv',retries =2,
                  retry_delay = timedelta(seconds =15),dag=dag)

t2= PythonOperator(task_id='clean_raw_csv', python_callable=data_cleaner)

t3 = SQLExecuteQueryOperator(task_id ='create_table', conn_id=MYSQL_CONN_ID,sql="create_table.sql", dag=dag)

t4 = SQLExecuteQueryOperator(task_id ='insert_into_table', conn_id=MYSQL_CONN_ID,sql="insert_into_table.sql", dag=dag) 

t_cleanup = BashOperator(
    task_id='delete_old_reports',
    bash_command='rm -f /opt/airflow/data/csv_files/location_wise_profit.csv /opt/airflow/data/csv_files/store_wise_profit.csv',
    dag=dag
)

t5 = SQLExecuteQueryOperator(task_id ='select_from_table', conn_id=MYSQL_CONN_ID,sql="select_from_table.sql", dag=dag) 

t6 = BashOperator(
    task_id='move_file1',
    # 1. Removed the ~ 
    # 2. Used absolute paths
    # 3. Used {{ ds }} macro instead of %s yesterday
    bash_command=(
        'cat /opt/airflow/data/csv_files/location_wise_profit.csv && '
        'mv /opt/airflow/data/csv_files/location_wise_profit.csv '
        '/opt/airflow/data/csv_files/location_wise_profit_{{ macros.ds_add(ds, -2)}}.csv'
    ),
    dag=dag
)

t7 = BashOperator(
    task_id='move_file2',
    # 1. Removed the ~ 
    # 2. Used absolute paths
    # 3. Used {{ ds }} macro instead of %s yesterday
    bash_command=(
        'cat /opt/airflow/data/csv_files/store_wise_profit.csv && '
        'mv /opt/airflow/data/csv_files/store_wise_profit.csv '
        '/opt/airflow/data/csv_files/store_wise_profit_{{ macros.ds_add(ds, -2)}}.csv'
    ),
    dag=dag
)

# t6 = BashOperator(task_id='move_file1', bash_command='cat ~/opt/airflow/data/csv_files/location_wise_profit.csv && mv ~/opt/airflow/data/csv_files/location_wise_profit.csv ~//opt/airflow/data/csv_files/location_wise_profit_%s.csv' % yesterday)

# t7 = BashOperator(task_id='move_file2', bash_command='cat /opt/airflow/data/csv_files/store_wise_profit.csv && mv /opt/airflow/data/csv_files/store_wise_profit.csv' '/opt/airflow/data/csv_files/location_wise_profit_{{ macros.ds_add(ds, -2)}}.csv')

t8 = EmailOperator(task_id='send_email',
        to='yongyongcn@gmail.com',
        subject='Daily report generated',
        html_content=""" <h1>Congratulations! Your store reports are ready.</h1> """,
        files=['/opt/airflow/data/csv_files/location_wise_profit_{{ macros.ds_add(ds, -2)}}.csv', '/opt/airflow/data/csv_files/store_wise_profit_{{ macros.ds_add(ds, -2)}}.csv'], dag=dag)

# t9 = BashOperator(task_id='rename_raw', bash_command='mv /opt/airflow/data/csv_files/raw_store_transactions.csv /opt/airflow/data/csv_files/raw_store_transactions_{{ macros.ds_add(ds, -2)}}.csv', dag=dag)



t1>>t2>>t3>>t4>>t_cleanup>>t5>>[t6,t7]>>t8