
          insert into `dbt_test_airflow_snapshots`.`customers_snapshot_2` (`customer_id`, `name`, `tier`, `updated_at`, `dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`)
    select DBT_INTERNAL_SOURCE.`customer_id`,DBT_INTERNAL_SOURCE.`name`,DBT_INTERNAL_SOURCE.`tier`,DBT_INTERNAL_SOURCE.`updated_at`,DBT_INTERNAL_SOURCE.`dbt_updated_at`,DBT_INTERNAL_SOURCE.`dbt_valid_from`,DBT_INTERNAL_SOURCE.`dbt_valid_to`,DBT_INTERNAL_SOURCE.`dbt_scd_id`
    from `dbt_test_airflow_snapshots`.`customers_snapshot_2__dbt_tmp` as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'

      