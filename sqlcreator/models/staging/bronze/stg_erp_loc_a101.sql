SELECT
    *
FROM {{ source('bronze', 'erp_loc_a101') }}
