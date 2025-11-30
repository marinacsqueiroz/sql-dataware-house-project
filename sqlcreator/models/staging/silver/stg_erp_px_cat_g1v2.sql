WITH bronze AS (
    SELECT
        *
    FROM {{ source('bronze', 'erp_px_cat_g1v2') }}
)
SELECT
    bronze.*,
    {{ dbt.current_timestamp() }} AS dbt_create_date
FROM bronze
