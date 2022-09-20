WITH sorce_data AS (
SELECT TOTAL,
    NOK,
    OK,
    (TOTAL/OK-1)*100 AS PERCENT_DIFF, --(VF/VI-1)*100
    2 AS THRESHOLD, --2%
    {{validation_descript('NOK','TOTAL','ORDER_ITEM_PIDs da tabela ORDER_ITEMS não existem na coluna PID da CONTACTS')}} AS DESCRIPTION
FROM(
SELECT COUNT() AS TOTAL,
        countIf(ORDER_ITEM_PID_IN = 0) AS NOK,
        countIf(ORDER_ITEM_PID_IN = 1) AS OK
FROM
(
    SELECT ORDER_ITEM_PID,
        ORDER_ITEM_PID IN (SELECT PID FROM {{source('prod','CONTACTS')}} ) AS ORDER_ITEM_PID_IN
    FROM
    (
        SELECT DISTINCT ORDER_ITEM_PID
        FROM {{source('prod','ORDER_ITEMS')}}
        WHERE ORDER_ITEM_PID != 0
    )
)
)
)
SELECT * FROM sorce_data