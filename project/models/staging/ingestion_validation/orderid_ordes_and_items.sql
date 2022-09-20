WITH sorce_data AS (
SELECT TOTAL,
    NOK,
    OK,
    (TOTAL/OK-1)*100 AS PERCENT_DIFF, --(VF/VI-1)*100
    5 AS THRESHOLD,
    {{validation_descript('NOK','TOTAL','ORDER_IDs da tabela ORDER_ITEMS não existem na ORDERS')}} AS DESCRIPTION
FROM(
SELECT COUNT() AS TOTAL,
        countIf(ID_IN = 0) AS NOK,
        countIf(ID_IN = 1) AS OK
FROM
(
    SELECT ORDER_ID,
        ORDER_ID IN (SELECT ORDER_ID FROM {{source('prod','ORDERS')}}     {% if var('dates') == 'true' %}
        WHERE ORDER_DATE BETWEEN "{{ var('start_date') }}" AND "{{ var('end_date') }}"
    {% else %}
    {% endif %}) AS ID_IN
    FROM
    (
        SELECT DISTINCT ORDER_ID
        FROM {{ source('prod','ORDER_ITEMS') }}
    {% if var('dates') == 'true' %}
        WHERE ORDER_DATE BETWEEN "{{ var('start_date') }}" AND "{{ var('end_date') }}"
    {% else %}
    {% endif %}
    )
)
)
)
SELECT * FROM sorce_data