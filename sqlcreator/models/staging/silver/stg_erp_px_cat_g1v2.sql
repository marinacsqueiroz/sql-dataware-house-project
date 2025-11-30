{{ config(
    post_hook=[
      "ALTER TABLE {{ this }} ADD dwh_create_date DATETIME2 DEFAULT GETDATE()"
    ]
) }}

WITH bronze AS (
    SELECT
        *
    FROM {{ source('bronze', 'erp_px_cat_g1v2') }}
)
SELECT
    *
FROM bronze
