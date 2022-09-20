WITH orders_insights AS (
    WITH O AS (
        SELECT
            PID,
            ORDER_DATE,
            ORDER_STORE,
            ORDER_ID,
            ORDER_TOTAL,
            ORDER_DATE
        FROM {{source('prod', 'ORDERS')}}
        WHERE ORDER_VALID = 1
        AND PID > 0
        AND ORDER_DATE >= toDate(today () -1) 
    ),

    BUYERS AS (
        SELECT PID FROM O
    )

SELECT

	ORDER_DATE
	, ORDER_TOTAL
	, ORDER_STORE
	, PID AS CUSTOMER_FK
	, SUGGESTION_ID AS SUGGESTION_FK
	, CAMPAIGN_FK
	, CONTACT_CREATE_DATE AS CREATE_DATE

FROM (

    SELECT
        if(APP_FOLLOWUP.TYPE = 'user' OR ORDERS.TYPE = 'user' OR APP_SUGGESTIONS.TYPE = 'user', 'user', 'oto') AS CONTACT_TYPE,
        if(CONTACT_TYPE = 'user' AND SCHEDULED = 0, APP_FOLLOWUP.CONTACT_SELLER, APP_SUGGESTIONS.SELLER_FK) AS CONTACT_SELLER,
        if(CONTACT_TYPE = 'user' AND SCHEDULED = 0, APP_FOLLOWUP.CONTACT_STORE, APP_SUGGESTIONS.STORE_FK) AS CONTACT_STORE,
        if(CONTACT_TYPE = 'user' AND SCHEDULED = 0, APP_FOLLOWUP.CUSTOMER_FK, APP_SUGGESTIONS.CUSTOMER_FK) AS PID,
        toDate(if(CONTACT_TYPE = 'user' AND SCHEDULED = 0, APP_FOLLOWUP.CREATE_DATE, APP_SUGGESTIONS.DATE)) AS SUGGESTION_DATE,
        if(CONTACT_TYPE = 'user' AND SCHEDULED = 0, APP_FOLLOWUP.CAMPAIGN_FK, APP_SUGGESTIONS.CAMPAIGN_FK) AS CAMPAIGN_FK,
        if(CONTACT_TYPE = 'user' AND SCHEDULED = 0, APP_FOLLOWUP.SUGGESTION_ID, APP_SUGGESTIONS.SUGGESTION_ID) AS SUGGESTION_ID,
        if(CONTACT_TYPE = 'user' AND SCHEDULED = 0, APP_FOLLOWUP.ENABLED, APP_SUGGESTIONS.ENABLED) AS SUGGESTION_ENABLED,
        if(CONTACT_TYPE = 'user' AND SCHEDULED = 0, 0, APP_SUGGESTIONS.IGNORED) AS SUGGESTION_IGNORED,
        APP_SUGGESTIONS.SCHEDULED AS SUGGESTION_SCHEDULED,
        APP_FOLLOWUP.FOLLOWUP_ID AS FOLLOWUP_ID,
        APP_FOLLOWUP.ENABLED AS CONTACT_ENABLED,
        APP_FOLLOWUP.STATUS AS CONTACT_STATUS,
        APP_FOLLOWUP.PENDING AS CONTACT_PENDING,
        APP_FOLLOWUP.CREATE_DATE AS CONTACT_CREATE_DATE,
        ORDERS.ORDER_ID AS ORDER_ID,
        ORDERS.ORDER_STORE AS ORDER_STORE,
        ORDERS.ORDER_DATE AS ORDER_DATE,
        ORDERS.ORDER_TOTAL AS ORDER_TOTAL,
        ORDERS.WINDOW AS WINDOW

    FROM (
        SELECT
            SUGGESTION_ID,
            SELLER_FK,
            STORE_FK,
            CUSTOMER_FK,
            DATE,
            if(CAMPAIGN_FK = toUUIDOrZero('') OR SCHEDULED = 1, 'user', 'oto') AS TYPE,
            CAMPAIGN_FK,
            ENABLED,
            IGNORED,
            SUGGESTION_ID IN (
                SELECT SUGGESTION_FK
                FROM {{source('prod', 'APP_SCHEDULE')}}
                WHERE ENABLED = 1
                    AND DATE >= addDays(toDate(today () -1), -15) -- start date / lookback
                    AND SUGGESTION_FK != toUUIDOrZero('')
            ) AS SCHEDULED

        FROM {{source('prod', 'APP_SUGGESTIONS')}}
        WHERE DATE BETWEEN addDays(toDate(today () -1), -15) AND toDate(today () -1) -- start date / end date / lookback

    ) AS APP_SUGGESTIONS

    FULL OUTER JOIN (

        -- APP_FOLLOWUP

        SELECT TYPE,
            CUSTOMER_FK,
            CONTACT_STORE,
            SUGGESTION_ID,
            CONTACT_SELLER,
            FOLLOWUP_ID,
            ENABLED,
            STATUS,
            PENDING,
            CREATE_DATE,
            CAMPAIGN_FK

        FROM (

            SELECT FOLLOWUP_ID,
                'oto' AS TYPE,
                SUGGESTION_FK AS SUGGESTION_ID,
                CUSTOMER_FK,
                STORE_FK AS CONTACT_STORE,
                SELLER_FK AS CONTACT_SELLER,
                CAMPAIGN_FK,
                ENABLED,
                STATUS,
                PENDING,
                CREATE_DATE

            FROM {{source('prod', 'APP_FOLLOWUP')}}

            WHERE ENABLED = 1
            AND NOT (
                isNull(SUGGESTION_FK)
                OR SUGGESTION_FK = toUUIDOrZero('')
                OR CAMPAIGN_FK = toUUIDOrZero('')
            )
            AND toDate(CREATE_DATE) BETWEEN addDays(toDate(today () -1), -15) AND toDate(today () -1) -- start date / end date / lookback

            ORDER BY CREATE_DATE DESC, FOLLOWUP_ID DESC
            LIMIT 1 BY SUGGESTION_ID

            UNION ALL

            SELECT FOLLOWUP_ID,
                'user' AS TYPE,
                if(
                    isNull(SUGGESTION_FK) OR SUGGESTION_FK = toUUIDOrZero(''),
                    generateUUIDv4(),
                    SUGGESTION_FK) AS SUGGESTION_ID,
                CUSTOMER_FK,
                STORE_FK AS CONTACT_STORE,
                SELLER_FK AS CONTACT_SELLER,
                CAMPAIGN_FK,
                ENABLED,
                STATUS,
                PENDING,
                CREATE_DATE

            FROM {{source('prod', 'APP_FOLLOWUP')}}

            WHERE ENABLED = 1
            AND (
                isNull(SUGGESTION_FK)
                OR SUGGESTION_FK = toUUIDOrZero('')
                OR CAMPAIGN_FK = toUUIDOrZero('')
            )
            AND toDate(CREATE_DATE) BETWEEN addDays(toDate(today () -1), -15) AND toDate(today () -1) -- start date / end date / lookback


        ) AS APP_FOLLOWUP

    ) AS APP_FOLLOWUP
    ON APP_SUGGESTIONS.SUGGESTION_ID = APP_FOLLOWUP.SUGGESTION_ID

    FULL OUTER JOIN (

        -- ORDERS

        SELECT TYPE,
            SUGGESTION_ID,
            FOLLOWUP_ID,
            ORDER_ID,
            ORDER_DATE,
            ORDER_TOTAL,
            ORDER_STORE,
            dateDiff('day',  APP_FOLLOWUP.CREATE_DATE, ORDERS.ORDER_DATE) AS WINDOW,
            CAMPAIGN_FK

        FROM O AS ORDERS

        INNER JOIN (

            SELECT FOLLOWUP_ID,
                'oto' AS TYPE,
                SUGGESTION_FK AS SUGGESTION_ID,
                CUSTOMER_FK,
                STORE_FK AS CONTACT_STORE,
                SELLER_FK AS CONTACT_SELLER,
                CAMPAIGN_FK,
                ENABLED,
                STATUS,
                PENDING,
                CREATE_DATE

            FROM {{source('prod', 'APP_FOLLOWUP')}}

            WHERE ENABLED = 1
            AND NOT (isNull(SUGGESTION_FK) OR SUGGESTION_FK = toUUIDOrZero(''))
            AND toDate(CREATE_DATE) BETWEEN addDays(toDate(today () -1), -15) AND toDate(today () -1)  -- start date / end date / lookback
            AND CUSTOMER_FK IN BUYERS
            ORDER BY CREATE_DATE DESC, FOLLOWUP_ID DESC
            LIMIT 1 BY SUGGESTION_ID

            UNION ALL

            SELECT FOLLOWUP_ID,
                'user' AS TYPE,
                generateUUIDv4() AS SUGGESTION_ID,
                CUSTOMER_FK,
                STORE_FK AS CONTACT_STORE,
                SELLER_FK AS CONTACT_SELLER,
                CAMPAIGN_FK,
                ENABLED,
                STATUS,
                PENDING,
                CREATE_DATE

            FROM {{source('prod', 'APP_FOLLOWUP')}}

            WHERE ENABLED = 1
            AND (isNull(SUGGESTION_FK) OR SUGGESTION_FK = toUUIDOrZero(''))
            AND toDate(CREATE_DATE) BETWEEN addDays(toDate(today () -1), -15) AND toDate(today () -1)  -- start date / end date / lookback
            AND CUSTOMER_FK IN BUYERS
        ) AS APP_FOLLOWUP
        ON ORDERS.PID = APP_FOLLOWUP.CUSTOMER_FK

        WHERE toDate(APP_FOLLOWUP.CREATE_DATE) >= addDays(toDate(today () -1), -15)
        AND WINDOW BETWEEN 0 AND 15 -- lookback

        ORDER BY APP_FOLLOWUP.CREATE_DATE DESC, APP_FOLLOWUP.FOLLOWUP_ID DESC
        LIMIT 1 BY ORDER_DATE, ORDER_STORE, ORDER_ID

    ) AS ORDERS
    ON APP_FOLLOWUP.FOLLOWUP_ID = ORDERS.FOLLOWUP_ID

    WHERE ORDER_DATE = toDate(today () -1) 

) AS SUGGESTIONS

WHERE ORDER_TOTAL > 0
	AND SUGGESTION_IGNORED = 0
	AND SUGGESTION_ENABLED = 1
	AND SUGGESTION_SCHEDULED = 0
	AND CONTACT_ENABLED = 1
	AND CONTACT_STATUS = 1


 )

SELECT * FROM orders_insights
