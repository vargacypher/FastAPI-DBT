{% test test_threshold_validation(model, column_name) %}

    select *
    from {{ model }}
    where {{ column_name }} > THRESHOLD

{% endtest %}