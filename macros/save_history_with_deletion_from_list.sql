{% macro save_history_deletion_from_list(
    input_rel,
    key_column,
    diff_column,
    del_rel,
    del_key_column,
    load_ts_column = 'LOAD_TS_UTC',
    input_filter_expr = 'true',
    high_watermark_column = none,
    heigh_watermark_test = '>=',
    order_by_expr = none
) -%}
{{ config(materialized='incremental') }}

WITH
{% if is_incremental() -%}
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
    LEFT OUTER JOIN current_from_history as h
    ON i.{{ diff_column }} = h.{{ diff_column }}
    WHERE {{ input_filter_expr }}
      AND NOT h.DELETED
      AND h.{{ diff_column }} IS NULL
    {%- if high_watermark_column %}
      AND {{ high_watermark_column }} {{ heigh_watermark_test }} (SELECT MAX({{ high_watermark_column }}) FROM {{ this }})
    {%- endif %}
),

delete_from_hist as (
    SELECT
        * EXCLUDE(DELETED),
        true as DELETED
    FROM current_from_history
    WHERE NOT DELETED
      AND {{ key_column }} IN (SELECT {{ key_column }} FROM {{ delete_rel }})
),

changed_to_store as (
    SELECT * FROM current_from_history
    UNION ALL
    SELECT * FROM load_from_inputs
)

{%- else %}

changed_to_store as (
    SELECT
        *,
        false as DELETED
    FROM {{ input_rel }}
    WHERE {{ input_filter_expr }}
)

{%- endif %}
{%- endmacro %}