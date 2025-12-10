
  
    

  create  table
    `dbt_test_airflow`.`join_cus_order_prod__dbt_tmp`
    
    
      as
    
    (
      with cus as (
 SELECT
  * from `dbt_test_airflow`.`stg_customer`

)
 
,ord as (
 SELECT
  * from `dbt_test_airflow`.`stg_order`
)
,prod as (
 SELECT
  * from `dbt_test_airflow`.`stg_products`
)

select  cus.name, cus.tier,         
       ord.order_status, ord.total_amount,
        prod.product_name, prod.category
     
from cus 
join ord on cus.customer_id = ord.customer_id
left join prod on trim(ord.product_id) = trim(prod.product_id)
    )

  