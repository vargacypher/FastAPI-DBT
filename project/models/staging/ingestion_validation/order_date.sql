WITH sorce_data AS (
SELECT TOTAL,
    NOK,
    OK,
    (TOTAL/OK-1)*100 AS PERCENT_DIFF, --(VF/VI-1)*100
    0 AS THRESHOLD,
    {{validation_descript('NOK','TOTAL','Coluna ORDER_DATE da tabela ORDERS não contém valores entre as datas de 2000-01-01 e hoje')}} AS DESCRIPTION
FROM(
SELECT COUNT() AS TOTAL,
        countIf(DATE_FUTURE = 1 OR DATE_PAST = 1) AS NOK,
        countIf(DATE_FUTURE = 0 AND DATE_PAST = 0) AS OK
FROM
(
    SELECT ORDER_DATE,
        ORDER_DATE > toDate(now()) AS DATE_FUTURE,
        ORDER_DATE < '2000-01-01' AS DATE_PAST
    FROM
    (
        SELECT DISTINCT ORDER_DATE
        FROM {{source('prod','ORDERS')}}
    )
)
)
)
SELECT * FROM sorce_data