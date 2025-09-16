with current_from_snapshot as (
    SELECT * EXCLUDE (DBT_SCD_ID, DBT_UPDATED_AT, DBT_VALID_FROM, DBT_VALID_TO)
    FROM {{ ref('SNSH_EXCHANGE') }}
    WHERE DBT_VALID_TO IS NULL
)
SELECT * FROM current_from_snapshot