 {% snapshot customers_snapshot %}
 {{
  config(
    target_schema='dbt_test_airflow_snapshots',
    materialized='snapshot',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at'
  )
}}

select * from {{ ref('stg_customer') }}

{% endsnapshot %}
