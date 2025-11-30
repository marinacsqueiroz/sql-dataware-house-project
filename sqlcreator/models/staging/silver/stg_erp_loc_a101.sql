WITH bronze AS (
    SELECT
        *
    FROM {{ source('bronze', 'erp_loc_a101') }}
)
SELECT
    bronze.*,
    {{ dbt.current_timestamp() }} AS dbt_create_date
FROM bronze
