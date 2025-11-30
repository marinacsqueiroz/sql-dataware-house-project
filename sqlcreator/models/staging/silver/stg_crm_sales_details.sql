WITH bronze AS (
    SELECT
        *
    FROM {{ source('bronze', 'crm_sales_details') }}
)
SELECT
    bronze.*,
    {{ dbt.current_timestamp() }} AS dbt_create_date
FROM bronze
