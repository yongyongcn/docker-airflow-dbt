from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="zmysql_operator_etl_example",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["mysql", "etl"],
) as dag:
    
    # 1. Define the SQL connection ID (Must match the ID in the Airflow UI)
    MYSQL_CONN_ID = "mysql_airflow_conn_test"
    DATABASE_NAME = "dbt_test_airflow"
    
    # Task 1: Create the database if it doesn't exist
    create_database = SQLExecuteQueryOperator(
        task_id="create_database_if_not_exists",
        conn_id=MYSQL_CONN_ID,
        sql=f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME};"
    )

    # Task 2: Create a table inside the newly created or existing database
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=MYSQL_CONN_ID,
        # The USE command ensures subsequent SQL runs against the correct database
        sql=f"""
            USE {DATABASE_NAME};
            CREATE TABLE IF NOT EXISTS inventory_data_yy (
                product_id INT PRIMARY KEY,
                product_name VARCHAR(255) NOT NULL,
                stock_level INT
            );
            TRUNCATE TABLE inventory_data_yy;
        """
    )

    # Task 3: Insert sample data into the table
    insert_data = SQLExecuteQueryOperator(
        task_id="insert_sample_data",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            USE {DATABASE_NAME};
            INSERT INTO inventory_data_yy (product_id, product_name, stock_level)
            VALUES (101, 'Airflow T-Shirt', 50),
                   (102, 'Docker Mug', 15),
                   (103, 'Victoria Laptop', 100);
        """
    )
    
    # Task 4: Select and display data (Output usually goes to Airflow logs)
    # The result will be fetched and printed to the task instance logs.
    select_data = SQLExecuteQueryOperator(
        task_id="select_and_log_data",
        conn_id=MYSQL_CONN_ID,
        sql=f"SELECT * FROM {DATABASE_NAME}.inventory_data_yy WHERE stock_level > 80;",
        show_return_value_in_logs=True
    )

    # Define the task dependencies
    create_database >> create_table >> insert_data >> select_data