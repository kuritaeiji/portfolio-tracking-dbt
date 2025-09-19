{% macro run_migrations(database = target.database, schema_prefix = target.schema) -%}
{% do log(
    "Runnning V001_drop_example_table migration with database = "
     ~ database
     ~ ", schema_prefix = "
     ~ schema_prefix, info=True) %}
{% do run_query(V001_drop_example_table(database, schema_prefix)) %}
{#% do log("no migrations to run.", info=True) %#}
-- マイグレーションが無いときは上のコメントを外して使う
{%- endmacro %}