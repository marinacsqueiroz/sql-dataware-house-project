SELECT
    *
FROM {{ source('bronze', 'crm_sales_details') }}
