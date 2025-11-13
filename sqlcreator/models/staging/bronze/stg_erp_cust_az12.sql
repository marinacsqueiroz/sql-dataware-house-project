SELECT
    *
FROM {{ source('bronze', 'erp_cust_az12') }}
