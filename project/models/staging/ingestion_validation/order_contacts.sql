WITH sorce_data AS (
SELECT TOTAL,
    NOK,
    OK,
    ((TOTAL/OK)-1)*100 AS PERCENT_DIFF, --(VF/VI-1)*100
    2 AS THRESHOLD, --2%
    {{validation_descript('NOK','TOTAL','Clientes da tabela ORDERS não existem na CONTACTS')}} AS DESCRIPTION
FROM(
SELECT COUNT() AS TOTAL,
        countIf(PID_IN = 0) AS NOK,
        countIf(PID_IN = 1) AS OK
FROM
(
    SELECT PID,
        PID IN (SELECT PID FROM {{source('prod','CONTACTS')}}) AS PID_IN
    FROM
    (
        SELECT DISTINCT PID
        FROM {{source('prod','ORDERS')}}
        WHERE PID != 0
    )
)
)
)
SELECT * FROM sorce_data