
WITH SENTIMENT AS (
    SELECT 
        id,
        article_id,
        sentiment_mark,
        CASE sentiment_poilievre
            WHEN 'N/A' THEN NULL
            ELSE sentiment_poilievre
        END
        AS sentiment_poilievre,
        '{{ run_started_at }}' AS LOAD_TS_UTC 
    FROM {{ source("news_source_table", "sentiment_raw") }}
)

SELECT * FROM SENTIMENT