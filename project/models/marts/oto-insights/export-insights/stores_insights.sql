WITH stores_insights AS (
	SELECT

		store_id AS STORE_ID,
		store_name AS STORE_NAME,
		store_state AS STORE_STATE,
		store_city AS STORE_CITY

	FROM {{ source( 'prod', 'STORE_DETAIL') }}
)

SELECT * FROM stores_insights
