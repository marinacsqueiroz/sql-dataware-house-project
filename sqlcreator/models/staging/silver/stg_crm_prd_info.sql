{{ config(
    post_hook=[
      "ALTER TABLE {{ this }} ADD dwh_create_date DATETIME2 DEFAULT GETDATE()"
    ]
) }}

WITH bronze AS (
    SELECT
        *
    FROM {{ source('bronze', 'crm_prd_info') }}
)
SELECT
    *
FROM bronze
