{#- Generic tests used in the model YAML (no package dependencies, so dbt runs offline). -#}

{% test expression_is_true(model, expression, where=None) %}
select * from {{ model }}
where not ({{ expression }})
{% if where %} and ({{ where }}) {% endif %}
{% endtest %}

{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}

{% test between(model, column_name, min_value, max_value) %}
select * from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }})
{% endtest %}

{% test row_count_matches(model, compare_model, compare_where=None) %}
with a as (select count(*) as n from {{ model }}),
     b as (select count(*) as n from {{ compare_model }} {% if compare_where %} where {{ compare_where }} {% endif %})
select a.n as model_rows, b.n as compare_rows from a cross join b where a.n <> b.n
{% endtest %}
