{{ config(
    materialized='table',
    schema='marts'
) }}

select
    TEA_ID,
    TOTAL_TRANSACTIONS,
    TOTAL_QUANTITY_SOLD,
    TOTAL_REVENUE,
    CURRENT_TIMESTAMP as METRICS_GENERATED_AT
from {{ ref('int_tea_metrics') }}

