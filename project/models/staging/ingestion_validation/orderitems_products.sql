WITH sorce_data AS (
SELECT TOTAL,
    NOK,
    OK,
    (TOTAL/OK-1)*100 AS PERCENT_DIFF, --(VF/VI-1)*100
    0 AS THRESHOLD,
    {{validation_descript('NOK','TOTAL','Dados de produtos das colunas da ORDER_ITEMS  não  batem com a PRODUCTS')}} AS DESCRIPTION
FROM(
SELECT COUNT() AS TOTAL,
        countIf(
                NOT PRODUCT_ID_EQ OR
                NOT PRODUCT_NAME_ASCII_EQ OR
                NOT SIZE_ASCII_EQ OR
                NOT CATEGORY_ASCII_EQ OR
                NOT SUBCATEGORY_ASCII_EQ OR
                NOT GENDER_ASCII_EQ OR
                NOT LINE_ASCII_EQ OR
                NOT COLOR_ASCII_EQ OR
                NOT BRAND_ASCII_EQ
        )
        AS NOK,
        countIf(
                PRODUCT_ID_EQ AND
                PRODUCT_NAME_ASCII_EQ AND
                SIZE_ASCII_EQ AND
                CATEGORY_ASCII_EQ AND
                SUBCATEGORY_ASCII_EQ AND
                GENDER_ASCII_EQ AND
                LINE_ASCII_EQ AND
                COLOR_ASCII_EQ AND
                BRAND_ASCII_EQ
        )
        AS OK
FROM
(
        SELECT ORDER_DATE,
                ORDER_STORE,
                ORDER_ID,
                ORDER_ITEMS.PRODUCT_ID = PRODUCTS.PRODUCT_ID AS PRODUCT_ID_EQ,
                ORDER_ITEMS.PRODUCT_NAME = PRODUCTS.PRODUCT_NAME_ASCII AS PRODUCT_NAME_ASCII_EQ,
                ORDER_ITEMS.PRODUCT_SIZE = PRODUCTS.SIZE_ASCII AS SIZE_ASCII_EQ,
                ORDER_ITEMS.PRODUCT_CATEGORY = PRODUCTS.CATEGORY_ASCII AS CATEGORY_ASCII_EQ,
                ORDER_ITEMS.PRODUCT_SUBCATEGORY = PRODUCTS.SUBCATEGORY_ASCII AS SUBCATEGORY_ASCII_EQ,
                ORDER_ITEMS.PRODUCT_GENDER = PRODUCTS.GENDER_ASCII AS GENDER_ASCII_EQ,
                ORDER_ITEMS.PRODUCT_LINE = PRODUCTS.LINE_ASCII AS LINE_ASCII_EQ,
                ORDER_ITEMS.PRODUCT_COLOR = PRODUCTS.COLOR_ASCII AS COLOR_ASCII_EQ,
                ORDER_ITEMS.PRODUCT_BRAND = PRODUCTS.BRAND_ASCII AS BRAND_ASCII_EQ
        FROM
        (
                SELECT *
                FROM {{source('prod','ORDER_ITEMS')}}
                {% if var('dates') == 'true' %}
                        WHERE ORDER_DATE BETWEEN "{{ var('start_date') }}" AND "{{ var('end_date') }}"
                {% else %}
                {% endif %}
        )
        AS ORDER_ITEMS
        ALL LEFT JOIN {{source('prod','PRODUCTS')}}  USING PRODUCT_SKU
)
)
)
SELECT * FROM sorce_data