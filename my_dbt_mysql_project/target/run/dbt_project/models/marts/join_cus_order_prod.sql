
  
    

  create  table
    `dbt_test_airflow`.`join_cus_order_prod__dbt_tmp`
    
    
      as
    
    (
      with cus as (
 SELECT
  * from `dbt_test_airflow`.`stg_customer`

)
 

select * from cus
    )

  