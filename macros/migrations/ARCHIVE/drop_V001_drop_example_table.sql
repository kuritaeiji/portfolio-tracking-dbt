{% macro V001_drop_example_table(database = target.database, schema_prefix = target.schema) -%}
DROP TABLE IF EXISTS {{ database }}.{{ schema }}_REFINED.REF_ABC_BANK_POSITION;
{%- endmacro %}