WITH campaigns AS (
    SELECT

        ACTION_ID,
        ACTION_NAME,
        CAMPAIGN_ID,
        CAMPAIGN_NAME,
        CREATE_DATE

    FROM {{source('prod', 'APP_ACTIONS')}} A
    INNER JOIN (
        SELECT

            CAMPAIGN_ID,
            ACTION_FK ,
            CAMPAIGN_NAME,
            CREATE_DATE,
            ENABLED

        FROM {{source('prod', 'APP_CAMPAIGNS')}}
        ) C
    ON ACTION_ID = ACTION_FK
    WHERE
        A.ENABLED = 1
        AND C.ENABLED = 1
)

SELECT * FROM campaigns
