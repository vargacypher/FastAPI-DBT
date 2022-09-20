WITH sorce_data AS (
SELECT TOTAL,
    NOK,
    OK,
    (TOTAL/OK-1)*100 AS PERCENT_DIFF, --(VF/VI-1)*100
    5 AS THRESHOLD, --5%
    {{validation_descript('NOK','TOTAL','Pedidos da tabela ORDERS  não  possuem filhos na ORDER_ITEMS pela chave ORDER_DATE, ORDER_STORE e ORDER_ID')}} AS DESCRIPTION
FROM(
SELECT COUNT() AS TOTAL,
	countIf(PK_IN = 0) AS NOK,
	countIf(PK_IN = 1) AS OK
FROM
(
    SELECT ORDER_DATE,
        ORDER_STORE,
        ORDER_ID,
        (ORDER_DATE, ORDER_STORE, ORDER_ID) IN (
            SELECT (ORDER_DATE, ORDER_STORE, ORDER_ID)
            FROM {{source('prod','ORDER_ITEMS')}}
            {% if var('dates') == 'true' %}
                WHERE ORDER_DATE BETWEEN "{{ var('start_date') }}" AND "{{ var('end_date') }}"
            {% else %}
            {% endif %}) AS PK_IN
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