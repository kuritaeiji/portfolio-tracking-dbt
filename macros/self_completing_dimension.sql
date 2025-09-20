{% macro self_completing_dimension(
    dim_rel,
    dim_key_column,
    dim_default_key_value = '-1',
    rel_columns_to_exclude = [],
    fact_defs = []
) -%}

{% do rel_columns_to_exclude.append(dim_key_column) -%}

WITH dim_base as (
    SELECT
        {{ dim_key_column }}
        , * EXCLUDE ({{ rel_columns_to_exclude|join(', ') }})
    FROM {{ dim_rel }}
),

fact_key_list as (
    {% if fact_defs|length > 0 -%}
        {% for fact_model_key in fact_defs -%}
            SELECT DISTINCT {{ fact_model_key['key'] }} as FOREIGN_KEY
            FROM {{ ref(fact_model_key['model']) }}
            WHERE {{ fact_model_key['key'] }} IS NOT NULL
                {%if not loop.last %} UNION {% endif %}
        {%- endfor %}
    {% else -%}
        SELECT NULL as FOREIGN_KEY WHERE false -- 空集合
    {%- endif %}
),

missing_keys as (
    SELECT fact_key_list.FOREIGN_KEY
    FROM fact_key_list
    LEFT OUTER JOIN dim_base
    ON fact_key_list.FOREIGN_KEY = dim_base.{{ dim_key_column }}
    WHERE dim_base.{{ dim_key_column }} IS NULL
),

default_record as (
    SELECT *
    FROM dim_base
    WHERE {{ dim_key_column }} = '{{ dim_default_key_value}}'
    LIMIT 1
),

dim_missing_entries as (
    SELECT
        mk.FOREIGN_KEY,
        dr.* EXCLUDE({{ dim_key_column }})
    FROM missing_keys as mk
    CROSS JOIN default_record as dr
)

SELECT * FROM dim_base
UNION ALL
SELECT * FROM dim_missing_entries
{%- endmacro %}