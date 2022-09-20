{% macro insights(name, databases_list) %}
    {% for database in databases_list %}
    (
    SELECT replace('{{database}}', 'context_', '') AS BRAND,
        * 
    FROM {{database}}.{{name}}
    )
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
{% endmacro %}
