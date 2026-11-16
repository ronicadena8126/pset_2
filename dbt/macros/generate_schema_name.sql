{# Usa el nombre de esquema configurado (BRONZE/SILVER/GOLD) tal cual,
   en lugar del default de dbt <target_schema>_<custom_schema>. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}{{ target.schema }}{%- else -%}{{ custom_schema_name | trim }}{%- endif -%}
{%- endmacro %}
