WITH sorce_data AS (
SELECT TOTAL,
    NOK,
    OK,
    (TOTAL/OK-1)*100 AS PERCENT_DIFF, --(VF/VI-1)*100
    5 AS THRESHOLD,
    {{validation_descript('NOK','TOTAL','PRODUCT_SKUs da tabela ORDER_ITEMS  não  existem na PRODUCTS')}} AS DESCRIPTION
FROM(
SELECT COUNT() AS TOTAL,
        countIf(SKU_IN = 0) AS NOK,
        countIf(SKU_IN = 1) AS OK
FROM
(
    SELECT PRODUCT_SKU,
        PRODUCT_SKU IN (SELECT PRODUCT_SKU FROM {{source('prod','PRODUCTS')}}) AS SKU_IN
    FROM
    (
        SELECT DISTINCT PRODUCT_SKU
        FROM {{source('prod','ORDER_ITEMS')}}
    )
)
)
)
SELECT * FROM sorce_data