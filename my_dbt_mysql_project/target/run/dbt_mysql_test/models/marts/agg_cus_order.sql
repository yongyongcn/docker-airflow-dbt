
  create view `dbt_test_airflow`.`agg_cus_order__dbt_tmp`
    
    
  as (
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

-- SELECT
-- cus.name as customer_name, cus.tier as customer_tier, ord.order_status, ord.total_amount, prd.product_name, prd.category
-- from  cus 
-- join ord on cus.customer_id = ord.customer_id
-- join prd on ord.product_id = prd.product_id
  );