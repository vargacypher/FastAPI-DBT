WITH sorce_data AS (
SELECT TOTAL,
    NOK,
    OK,
    (TOTAL/OK-1)*100 AS PERCENT_DIFF, --(VF/VI-1)*100
    0 AS THRESHOLD,
    {{validation_descript('NOK','TOTAL','Valores da coluna ORDER_STORE_TYPE não batem entre a ORDER_ITEMS e a ORDERS')}} AS DESCRIPTION
FROM(
SELECT COUNT() AS TOTAL,
        countIf(ORDER_ITEMS.ORDER_STORE_TYPE != ORDERS.ORDER_STORE_TYPE) AS NOK,
        countIf(ORDER_ITEMS.ORDER_STORE_TYPE = ORDERS.ORDER_STORE_TYPE) AS OK
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