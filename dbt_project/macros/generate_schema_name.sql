{#
  dbt 預設會把 schema 組成「<target schema>_<custom schema>」，例如 gold_gold。
  覆寫成：有指定 +schema 就直接用它，沒有就用 profiles.yml 的預設值。
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}
