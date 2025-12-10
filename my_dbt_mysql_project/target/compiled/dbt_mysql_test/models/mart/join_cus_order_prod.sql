with cus as (
 SELECT
  * from
`dbt_test_airflow`.`stg_customer`

)
, ord as (
 SELECT
  * 
from `dbt_test_airflow`.`stg_order`
)       
, prd as (
 SELECT
  *         
from `dbt_test_airflow`.`stg_products`
)

select * from cus