
WITH SOURCES AS (
    SELECT DISTINCT
        source_id,
        news_source_name,
        news_source_country,
        bias
    FROM {{ ref('transformed') }}
)

SELECT * FROM SOURCES