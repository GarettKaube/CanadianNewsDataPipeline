
{{  config(
        sort ='publishedat'
    )
}}

WITH ARTICLES AS (
    SELECT DISTINCT ON (article_id)
        article_id,
        title,
        description,
        news_category,
        article_content,
        url,
        publishedat,
        source_id,
        creation_time
    FROM {{ ref("transformed") }}
),

-- Get the Latest creation_time for duplicate content 
-- latest_title AS (
--     SELECT article_content, MAX(creation_time) as latest
--     FROM  ARTICLES  
--     GROUP BY article_content
-- ),

-- -- Get the Latest creation_time for duplicate titles 
-- latest_content AS (
--     SELECT title, MAX(creation_time) as latest
--     FROM  ARTICLES  
--     GROUP BY title
-- ),


latest_art AS (
    SELECT article_id, MAX(creation_time) as latest
    FROM  ARTICLES  
    GROUP BY article_id
),


-- Fix changing titles and content for the same article
dedup_content AS (
    SELECT
        A.article_id AS article_id,
        A.title as title,
        A.description as description,
        A.news_category as news_category,
        A.article_content as article_content,
        A.url as url,
        A.publishedat AS publishedat,
        A.source_id as source_id
    FROM ARTICLES as A
    RIGHT JOIN latest_art AS LT ON
        LT.article_id = A.article_id
        and A.creation_time = LT.latest
)

-- CORRELATED AS (
--     SELECT a1.*
--     FROM ARTICLES as a1
--     WHERE a1.creation_time = (
--         SELECT MAX(a2.creation_time)
--         FROM ARTICLES as a2
--         WHERE a1.article_content= a2.article_content
--         GROUP BY a2.article_content
--     )
-- )


SELECT * FROM dedup_content