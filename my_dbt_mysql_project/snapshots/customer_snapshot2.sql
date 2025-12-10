{% snapshot customers_snapshot_2 %}

{{
  config(
    target_schema='dbt_test_airflow_snapshots',
    unique_key='customer_id',
    strategy='check',
    check_cols=['name', 'tier' ]
  )
}}

select * from {{ ref('stg_customer') }}

{% endsnapshot %}