WITH final_data AS (
    
    --CONTACTS
    SELECT * FROM {{ref('contatcs_validation')}}


    --STORES
    UNION ALL
    SELECT * FROM {{ref('order_store_type')}}
    UNION ALL
    SELECT * FROM {{ref('orders_stores')}}
    UNION ALL
    SELECT * FROM {{ref('orderitems_stores')}}

    --ORDERS
    UNION ALL
    SELECT * FROM {{ref('order_contacts')}}
    UNION ALL
    SELECT * FROM {{ref('order_date')}}
    UNION ALL
    SELECT * FROM {{ref('orders_orderitems')}}
    UNION ALL
    SELECT * FROM {{ref('uniq_oders')}}


    --ORDER_ITEMS
    UNION ALL
    SELECT * FROM {{ref('orderid_ordes_and_items')}}
    UNION ALL
    SELECT * FROM {{ref('orderitems_contacts')}}
    UNION ALL
    SELECT * FROM {{ref('orderitems_date')}}
    UNION ALL
    SELECT * FROM {{ref('orderitems_orders')}}
    UNION ALL
    SELECT * FROM {{ref('orderitems_pid_orders')}}


    --PRODUCTS
    UNION ALL
    SELECT * FROM {{ref('orderitems_products')}}
    UNION ALL
    SELECT * FROM {{ref('product_sku')}}


)

SELECT * FROM final_data