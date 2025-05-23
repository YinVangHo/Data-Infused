-- Ensures no negative or zero prices in tea transactions
SELECT *
FROM {{ ref('stg_tea_transactions') }}
WHERE PRICE <= 0
