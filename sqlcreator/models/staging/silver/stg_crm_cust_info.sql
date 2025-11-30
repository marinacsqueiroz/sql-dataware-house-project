WITH bronze AS (
    SELECT
        *
    FROM {{ source('bronze', 'crm_cust_info') }}
)
SELECT
    bronze.*,
    {{ dbt.current_timestamp() }} AS dbt_create_date
FROM bronze
