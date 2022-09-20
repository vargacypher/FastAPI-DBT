WITH source_data AS (

   SELECT TOTAL,
    NOK,
    OK,
    (TOTAL/OK-1)*100 AS PERCENT_DIFF, --(VF/VI-1)*100
    2 AS THRESHOLD, --2%
    {{validation_descript('NOK','TOTAL','da coluna MOBILE_NUMBER da tabela CONTACTS não contém valores com 13 dígitos')}} AS DESCRIPTION
FROM(
    SELECT COUNT() AS TOTAL,
            countIf(VALID_LENGTH = 0) AS NOK,
            countIf(VALID_LENGTH = 1) AS OK
    FROM
    (
        SELECT MOBILE_NUMBER,
            length(MOBILE_NUMBER) = 13 AS VALID_LENGTH
        FROM
        (
            SELECT DISTINCT MOBILE_NUMBER
            FROM {{source('prod','CONTACTS')}}
            WHERE MOBILE_NUMBER != ''
        )
    )
)
    UNION ALL
    SELECT TOTAL,
    NOK,
    OK,
    (TOTAL/OK-1)*100 AS PERCENT_DIFF, --(VF/VI-1)*100
    0 AS THRESHOLD,
    {{validation_descript('NOK','TOTAL','da coluna GENDER da tabela CONTACTS não contém valores M, F, ou O')}} AS DESCRIPTION
FROM(
    SELECT COUNT() AS TOTAL,
            countIf(VALID_GENDER = 0) AS NOK,
            countIf(VALID_GENDER = 1) AS OK
    FROM
    (
        SELECT GENDER,
            GENDER IN ('M', 'F', 'O') AS VALID_GENDER
        FROM
        (
            SELECT DISTINCT GENDER
            FROM {{source('prod','CONTACTS')}}
            WHERE GENDER != ''
        )
    )
)
)

SELECT *
FROM source_data
