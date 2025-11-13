SELECT
    *
FROM {{ source('bronze', 'crm_cust_info') }}
