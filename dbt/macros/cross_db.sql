{#- Small cross-engine helpers: the same models run on DuckDB (local) and Athena/Trino (AWS). -#}

{% macro percentile(expr, p) -%}
    {{ return(adapter.dispatch('percentile')(expr, p)) }}
{%- endmacro %}
{% macro default__percentile(expr, p) -%} quantile_cont({{ expr }}, {{ p }}) {%- endmacro %}
{% macro athena__percentile(expr, p) -%} approx_percentile({{ expr }}, {{ p }}) {%- endmacro %}

{% macro seconds_between(start_ts, end_ts) -%}
    date_diff('second', {{ start_ts }}, {{ end_ts }})
{%- endmacro %}

{% macro iso_dow(ts) -%}
    {{ return(adapter.dispatch('iso_dow')(ts)) }}
{%- endmacro %}
{% macro default__iso_dow(ts) -%} isodow({{ ts }}) {%- endmacro %}
{% macro athena__iso_dow(ts) -%} day_of_week({{ ts }}) {%- endmacro %}

{% macro safe_divide(numerator, denominator) -%}
    (cast({{ numerator }} as double) / nullif(cast({{ denominator }} as double), 0))
{%- endmacro %}

{% macro license_filter() -%}
    hvfhs_license_num = '{{ var("license_num") }}'
{%- endmacro %}
