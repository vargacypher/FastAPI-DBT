WITH sorce_data AS (
SELECT TOTAL,
    NOK,
    OK,
    (TOTAL/OK-1)*100 AS PERCENT_DIFF, --(VF/VI-1)*100
    0 AS THRESHOLD, --0%
    {{validation_descript('NOK','TOTAL','ORDER_ITEM_PIDs da tabela ORDER_ITEMS  não  correspondem ao PID da ORDERS')}} AS DESCRIPTION
FROM(
SELECT COUNT() AS TOTAL,
        countIf(PID != ORDER_ITEM_PID) AS NOK,
        countIf(PID = ORDER_ITEM_PID) AS OK
FROM
(
    SELECT *
    FROM {{source('prod','ORDER_ITEMS')}}
    {% if var('dates') == 'true' %}
        WHERE ORDER_DATE BETWEEN "{{ var('start_date') }}" AND "{{ var('end_date') }}"
    {% else %}
    {% endif %}
)
AS ORDER_ITEMS ALL INNER JOIN
(
    SELECT *
    FROM {{source('prod','ORDERS')}}
    {% if var('dates') == 'true' %}
        WHERE ORDER_DATE BETWEEN "{{ var('start_date') }}" AND "{{ var('end_date') }}"
    {% else %}
    {% endif %}
)
AS ORDERS USING ORDER_DATE, ORDER_STORE, ORDER_ID
)
)
SELECT * FROM sorce_data