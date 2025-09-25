{% macro run_migration(migration_name, database, schema_prefix) -%}
{% if execute %}
{% do log("* Running " ~ migration_name ~ " migration with database = " ~ database ~ ", schema_prefix = " ~ schema_prefix, info=True) %}
{% set migration_macro = context.get(migration_name, none) %}
{% do run_query(migration_macro(database, schema_prefix)) if migration_macro else log("!! Macro " ~ migration_nameb ~ " not found. Skipping call.", info=True) %}
{% endif %}
{%- endmacro %}