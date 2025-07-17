{{ config(
    materialized='view',
    schema='staging'
) }}

with source as (
  select * from {{ source('tea_schema', 'tea_transactions') }}
),

renamed as (
  select
    transaction_id,
    tea_id::int as tea_id,
    customer_id,
    quantity::int as quantity,
    price::float as price,
    name as tea_name,
    category,
    country,
    to_timestamp(timestamp) as transaction_time
  from source
)

select * from renamed
