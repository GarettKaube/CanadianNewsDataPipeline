

WITH AUTHORS AS (
    SELECT DISTINCT
        author_id,
        FIRST_NAME,
        LAST_NAME,
        author_email,
        author_url
    FROM {{ ref('transformed') }}
)

SELECT * FROM AUTHORS