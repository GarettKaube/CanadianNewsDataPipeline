
WITH INIT AS (
    SELECT DISTINCT
        ARTICLE_AUTHOR_ID,
        AUTHOR_ID,
        ARTICLE_ID
    FROM {{ ref("transformed") }}
)

SELECT * FROM INIT