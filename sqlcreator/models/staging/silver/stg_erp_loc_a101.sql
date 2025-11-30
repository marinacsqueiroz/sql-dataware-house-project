{{ config(
    post_hook=[
      "ALTER TABLE {{ this }} ADD dwh_create_date DATETIME2 DEFAULT GETDATE()"
    ]
) }}

WITH bronze AS (
    SELECT
        *
    FROM {{ source('bronze', 'erp_loc_a101') }}
)
SELECT
    *
FROM bronze
