{% macro databases_list(table, exclude_databases) %}
    {%set database_query %}
        SELECT database 
        FROM {{source('system','tables')}} 
        WHERE name = '{{table}}'
            AND total_rows > 0
            AND database NOT IN ({{exclude_databases}})
    {% endset %}

    {% set results = run_query(database_query) %}

    {% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0].values() %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}
    {{ return(results_list) }}
{% endmacro %}
