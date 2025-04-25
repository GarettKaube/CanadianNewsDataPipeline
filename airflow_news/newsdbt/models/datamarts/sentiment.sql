WITH SENTIMENT AS (
    SELECT *
    FROM {{ ref("stg_sentiment") }}
)

SELECT * FROM SENTIMENT