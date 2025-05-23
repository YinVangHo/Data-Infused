{{ config(
    materialized='view',
    schema='staging'
) }}

with source as (
    select * from {{ source('tea_schema', 'tea_transactions') }}
),

renamed as (
    select
        transaction_id as TRANSACTION_ID,
        tea_id::int as TEA_ID,
        customer_id as CUSTOMER_ID,
        quantity::int as QUANTITY,
        price::float as PRICE,
        name as TEA_NAME,
        category as CATEGORY,
        country as COUNTRY,
        to_timestamp(timestamp) as TRANSACTION_TIME
    from source
)

select * from renamed
