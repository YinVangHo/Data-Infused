{{ config(
    materialized='incremental',
    unique_key='TEA_ID',
    schema='intermediate',
    incremental_strategy='merge'
) }}

with new_data as (
    select
        TEA_ID,
        QUANTITY,
        PRICE
    from {{ ref('stg_tea_transactions') }}
    {% if is_incremental() %}
      where TEA_ID not in (select TEA_ID from {{ this }})
    {% endif %}
),

aggregated as (
    select
        TEA_ID,
        count(*) as TOTAL_TRANSACTIONS,
        sum(QUANTITY) as TOTAL_QUANTITY_SOLD,
        sum({{ calc_revenue('QUANTITY', 'PRICE') }}) as TOTAL_REVENUE
    from new_data
    group by TEA_ID
)

select * from aggregated
