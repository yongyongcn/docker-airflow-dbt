from airflow.plugins_manager import AirflowPlugin
from airflow.sdk import BaseOperator
from airflow.sensors.base import BaseSensorOperator
import logging as log
from airflow.hooks.filesystem import FSHook
import os
import stat
# BaseHook is still in the core, but moved to the common 'hooks' package
from airflow.hooks.base import BaseHook

# MySQL and Postgres are now strictly part of their provider packages
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.utils.decorators import apply_defaults

class DataTransferOperator(BaseOperator):

    # @apply_defaults
    def __init__(self, source_file_path, dest_file_path, delete_list, *args, **kwargs):

        self.source_file_path = source_file_path
        self.dest_file_path = dest_file_path
        self.delete_list = delete_list
        super().__init__(*args, **kwargs)

    def execute(self, context):

        SourceFile = self.source_file_path
        DestinationFile = self.dest_file_path
        DeleteList = self.delete_list

        log.info("### custom operator execution starts ###")
        log.info('source_file_path: %s', SourceFile)
        log.info('dest_file_path: %s', DestinationFile)
        log.info('delete_list: %s', DeleteList)

        fin = open(SourceFile)
        fout = open(DestinationFile, "a")

        for line in fin:
            log.info('### reading line: %s', line)
            for word in DeleteList:
                log.info('### matching string: %s', word)
                line = line.replace(word, "")

            log.info('### output line is: %s', line)
            fout.write(line)

        fin.close()
        fout.close()

class FileCountSensor(BaseSensorOperator):

    # @apply_defaults
    def __init__(self, dir_path, conn_id, *args, **kwargs):
        self.dir_path = dir_path
        self.conn_id = conn_id
        super().__init__(*args, **kwargs)

    def poke(self,context):
        hook = FSHook(self.conn_id)
        basepath = hook.get_path()
        full_path = os.path.join(basepath, self.dir_path)
        self.log.info('poking location %s', full_path)
        try:
            for root, dirs, files in os.walk(full_path):
                #print(len(files))
                if len(files) >= 6:
                    return True
        except OSError:
            return False
        return False
    
class MySQLToPostgresHook(BaseHook):
    def __init__(self):
        print("##custom hook started##")

    def copy_table(self, mysql_conn_id, postgres_conn_id):

        print("### fetching records from MySQL table ###")
        mysqlserver = MySqlHook(mysql_conn_id)
        sql_query = "SELECT * from agg_prod "
        data = mysqlserver.get_records(sql_query)
        print(f"### Found {len(data)} records in MySQL ###") # Add this
        if not data:
            print("Warning: No data found in MySQL source!")

        print("### inserting records into Postgres table ###")
        postgresserver = PostgresHook(postgres_conn_id)
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS myschema_test.agg_prod (
                category VARCHAR(255),
                quantity INT,
                price FLOAT
            );
            """
        postgresserver.run(create_table_sql)        
        postgres_query = "INSERT INTO agg_prod VALUES(%s, %s, %s);"
        postgresserver.insert_rows(table='myschema_test.agg_prod', rows=data,autocommit=True)

        return True
    
# class DemoPlugin(AirflowPlugin):
#     name = "demo_plugin"
#     operators = [DataTransferOperator]
#     sensors = []
