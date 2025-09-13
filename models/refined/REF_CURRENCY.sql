with current_from_snapshot as (
    SELECT *
    FROM {{ ref('SNSH_CURRENCY') }}
    WHERE DBT_VALID_TO IS NULL
)
SELECT * FROM current_from_snapshot