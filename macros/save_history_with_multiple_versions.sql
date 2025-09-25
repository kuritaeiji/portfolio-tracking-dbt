{% macro save_history_with_multiple_versions(
    input_rel,
    key_column,
    diff_column,
    sort_expr,
    history_rel = this,
    load_ts_column = 'LOAD_TC_UTC',
    input_filter_expr = 'true',
    hitory_filter_expr = 'true',
    high_watermark_column = none,
    high_watermark_test = '>='
) -%}

{%- config(materialized='incremental') %}
{%- if execute and not flags.FULL_REFRESH %}
    {% set incremental_w_external_input = (history_rel != this) %} -- models/tmp/HIST_FOO_preview.sql のようなテスト用のモデルでマクロを使用する際にインクリメンタル更新されるようにする
{% endif -%}

WITH
{% if is_incremental() or incremental_w_external_input %}

{%- set selection_expr %}
    {{ key_column }}, {{ diff_column }}
    , ROW_NUMBER() OVER (PATITION_BY {{ key_column }}, {{ load_ts_column }} ORDER BY {{ sort_expr }}) as rn
    , COUNT(*) OVER (PATITION_BY {{ key_column }}, {{ load_ts_column }}) as cnt
{%- endset %}

current_from_history as (
    current_from_hitory(
        hitory_rel = history_rel,
        key_column = key_column,
        selection_expr = selection_expr,
        load_ts_column = load_ts_column,
        history_filter_expr = history_filter_expr,
        qualify_function = '(rn = cnt) AND ROW_NUMBER'
    )
),

load_from_input as (
    SELECT
        i.*
        , LAG(i.{{ diff_column }}) OVER (PARTITION BY i.{{ key_column }} ORDER BY {{ sort_expr }}) as PREV_DIFF
        , CASE
            WHEN PREV_DIFF IS NULL THEN IFF(h.{{ diff_column }} IS NULL, true, i.{{ diff_column }} != h.{{ diff_column }})
            ELSE i.{{ diff_column }} != PREV_DIFF
          END as TO_BE_STORED
    FROM {{ input_rel }} as i 
    LEFT OUTER JOIN {{ history_rel }} as h
    ON i.{{ key_column }} = h.{{ key_column }}
    WHERE {{ input_filter_expr }}
    {%- if high_watermark_column %}
        AND {{ high_watermark_column }} {{ high_watermark_test }} (SELECT MAX(load_ts_column) FROM {{ history_rel }})
    {%- endif %}
)

{% else %}
load_from_input as (
    SELECT
        i.*
        , LAG({{ diff_column }}) OVER (PARTITION BY {{ key_column }} ORDER BY {{ sort_expr }}) as PREV_DIFF
        , CASE
            WHEN PREV_DIFF IS NULL THEN true
            ELSE i.{{ diff_column }} != PREV_DIFF
          END as TO_BE_STORED
    FROM {{ input_rel }} as i
    WHERE {{ input_filter_expr }}
)
{% endif %}

SELECT * EXCLUDE(PREV_DIFF, TO_BE_STORED)
FROM load_from_input
WHERE TO_BE_STORED
ORDER BY {{ key_column }}, {{ sort_expr }}

{%- endmacro %}