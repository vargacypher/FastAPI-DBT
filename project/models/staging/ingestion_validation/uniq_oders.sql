
WITH sorce_data AS (
SELECT TOTAL,
    NOK,
    OK,
    (TOTAL/OK-1)*100 AS PERCENT_DIFF, --(VF/VI-1)*100
    0 AS THRESHOLD,
    {{validation_descript('NOK','TOTAL','Pedidos da tabela ORDERS  não são únicos pela chave ORDER_DATE, ORDER_STORE e ORDER_ID')}} AS DESCRIPTION
FROM(
SELECT COUNT() AS TOTAL,
        countIf(TOTAL_COUNT > 1) AS NOK,
        countIf(TOTAL_COUNT = 1) AS OK
FROM
(
    SELECT ORDER_DATE,
        ORDER_STORE,
        ORDER_ID,
        COUNT() AS TOTAL_COUNT
    FROM {{source('prod','ORDERS')}}
    {% if var('dates') == 'true' %}
        WHERE ORDER_DATE BETWEEN "{{ var('start_date') }}" AND "{{ var('end_date') }}"
    {% else %}
    {% endif %}
    GROUP BY ORDER_DATE, ORDER_STORE, ORDER_ID
)
)
)
SELECT * FROM sorce_data