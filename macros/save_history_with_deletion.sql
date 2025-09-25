{% macro save_history_with_deletion(
    input_rel,
    key_column,
    diff_column,
    load_ts_coloumn = 'LOAD_TS_UTC'
) -%}
{{ config(materialized='incremental') }}

WITH
{% if is_incremental() %}
current_from_history as (
    {{ current_from_history(
        history_rel = this,
        key_column = key_column,
        load_ts_column = load_ts_column
    ) }}
),

load_from_inputs as (
    SELECT
        i.*,
        false as DELETED
    FROM {{ input_rel }} as i 
    LEFT OUTER JOIN current_from_history as curr
    ON i.{{ diff_column }} = curr.{{ diff_column }}
    WHERE curr.{{ diff_column }} IS NULL
      AND NOT curr.DELETED
),

deleted_from_hist as (
    SELECT
        curr.* EXCLUDE (deleted, {{ load_ts_column }}),
        '{{ run_started_at }}'::TIMESTAMP_NTZ as LOSD_TS_UTC,
        true as DELETED
    FROM current_from_history as curr
    LEFT OUTER JOIN {{ input_rel }} as i
    ON curr.{{ key_column }} = i.{{ key_column }}
    WHERE i.{{ key_column }} IS NULL
      AND NOT curr.DELETED
),

changes_to_store as (
    SELECT * FROM load_from_inputs
    UNION ALL
    SELECT * FROM deleted_from_hist
)

{% else %}
changes_to_store as (
    SELECT
        i.*,
        false as DELETED
    FROM {{ input_rel }} as i
)
{% endif %}

SELECT * FROM changed_to_store
{%- endmacro %}