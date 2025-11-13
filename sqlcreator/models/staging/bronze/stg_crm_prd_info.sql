SELECT
    *
FROM {{ source('bronze', 'crm_prd_info') }}
