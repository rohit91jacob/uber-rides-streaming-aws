{#- Use the custom schema verbatim (staging / intermediate / marts) on DuckDB; on Athena prefix
    it with "uber_" so the Glue databases are uber_staging, uber_intermediate, uber_marts. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.type == 'athena' -%}
        uber_{{ custom_schema_name | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
