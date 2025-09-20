{% macro save_history(
    input_rel,
    key_column,
    diff_column,
    load_ts_column = 'LOAD_TS_UTC',
    input_filter_expr = 'true',
    history_filter_expr = 'true',
    high_watermark_column = none,
    high_watermark_test = '>=',
    order_by_expr = none)
-%}

{{ config(materialized='incremental') }}

with
{% if is_incremental() -%}
current_from_history as (
    -- 各 HKEY の現行 HDIFF を取得
    {{ current_from_history(
        history_rel = this,
        key_column = key_column,
        selection_expr = diff_column,
        load_ts_column = load_ts_column,
        history_filter_expr = history_filter_expr
    ) }}
),
load_from_input as (
    -- 入力と HIST の HDIFF を結合し、未保存の行（新規/変更）だけ残す
    SELECT i.*
    FROM {{ input_rel }} as i
    LEFT OUTER JOIN current_from_history as h
    ON i.{{ diff_column }} = h.{{ diff_column }}
    WHERE h.{{ diff_column }} IS NULL
      AND {{ input_filter_expr }}
    {%- if high_watermark_column %}
      AND {{ high_watermark_column }} {{ high_watermark_test }} (SELECT MAX({{ high_watermark_column }}) FROM {{ this }})
    {%- endif %}
)
{%- else %}
load_from_input as (
    -- 入力を全件ロード
    SELECT *
    FROM {{ input_rel }}
    WHERE {{ input_filter_expr }}
)
{%- endif %}
SELECT * FROM load_from_input
{%- if order_by_expr %}
ORDER BY {{ order_by_expr }}
{%- endif %}
{%- endmacro %}