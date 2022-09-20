
{% macro validation_descript(column0,column1,string) %}
    concat(toString({{column0}}),' de ',toString({{column1}}),' {{string}} ')
{% endmacro %}