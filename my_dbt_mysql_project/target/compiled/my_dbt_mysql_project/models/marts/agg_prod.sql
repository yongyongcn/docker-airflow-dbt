with joined as (
 SELECT
  * from `dbt_test_airflow`.`join_cus_order_prod`

)

select category,
       count(*) as total_orders,
       sum(total_amount) as total_revenue
from joined
group by category
order by total_revenue desc