WITH sorce_data AS (
SELECT TOTAL,
    NOK,
    OK,
    (TOTAL/OK-1)*100 AS PERCENT_DIFF, --(VF/VI-1)*100
    0 AS THRESHOLD,
    {{validation_descript('NOK','TOTAL','ORDER_STOREs da tabela ORDER_ITEMS não  existem na STORES')}} AS DESCRIPTION
FROM(
SELECT COUNT() AS TOTAL,
        countIf(STORE_IN = 0) AS NOK,
        countIf(STORE_IN = 1) AS OK
FROM
(
    SELECT STORE_ID,
        STORE_ID IN (SELECT store_id FROM {{source('prod','STORE_DETAIL')}} ) AS STORE_IN
    FROM
    (
        SELECT DISTINCT ORDER_STORE AS STORE_ID
        FROM {{source('prod','ORDER_ITEMS')}}
    )
)
)
)
SELECT * FROM sorce_data