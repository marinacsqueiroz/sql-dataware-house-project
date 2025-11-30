WITH bronze AS (
    SELECT
        *
    FROM {{ source('bronze', 'erp_cust_az12') }}
)
SELECT
    bronze.*,
    {{ dbt.current_timestamp() }} AS dbt_create_date
FROM bronze
