
  create view `dbt_test_airflow`.`stg_order__dbt_tmp`
    
    
  as (
    SELECT
  *
from `dbt_test_airflow_seeds`.`order`
  );