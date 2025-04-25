{{  config(
        materialized="view",
        sort ='publishedat'
    )
}}

WITH raw as (
    SELECT  id as ID,                       
        NEWS_SOURCE_NAME::VARCHAR as NEWS_SOURCE_NAME,          
        NEWS_SOURCE_COUNTRY::VARCHAR as NEWS_SOURCE_COUNTRY,    
        NEWS_CATEGORY::VARCHAR as NEWS_CATEGORY,                
        author::VARCHAR as AUTHOR,
        author_email::VARCHAR as AUTHOR_EMAIL,
        author_url::VARCHAR as AUTHOR_URL,                     
        title::VARCHAR as TITLE,                      
        description::VARCHAR as DESCRIPTION,          
        url::VARCHAR as URL,                          
        publishedat::TIMESTAMP as PUBLISHEDAT,          
        article_content::TEXT as ARTICLE_CONTENT,
        bias AS BIAS,
        CREATION_TIME  AS CREATION_TIME  
    FROM {{ ref("stg_articles") }}
),

-- Back Fill news articles content that failed to be retreieved
BACKFILL_CONTENT AS (
    SELECT
        r.id as ID, 
        r.NEWS_SOURCE_NAME AS NEWS_SOURCE_NAME,          
        r.NEWS_SOURCE_COUNTRY AS NEWS_SOURCE_COUNTRY,    
        r.NEWS_CATEGORY AS NEWS_CATEGORY,                
        r.AUTHOR AS AUTHOR,
        r.AUTHOR_EMAIL AS AUTHOR_EMAIL,
        r.AUTHOR_URL AS AUTHOR_URL,                     
        r.TITLE AS TITLE,                      
        r.DESCRIPTION AS DESCRIPTION,          
        r.URL AS URL,                          
        r.PUBLISHEDAT AS PUBLISHEDAT,          
        r.BIAS AS BIAS,
        r.CREATION_TIME AS CREATION_TIME,
        CASE 
            WHEN r.ARTICLE_CONTENT IS NULL THEN (
                SELECT DISTINCT 
                    r2.ARTICLE_CONTENT
                FROM raw as r2
                WHERE r.AUTHOR = r2.AUTHOR AND 
                    r.AUTHOR_EMAIL = r2.AUTHOR_EMAIL AND
                    r.TITLE = r2.TITLE AND 
                    r.NEWS_SOURCE_NAME = r2.NEWS_SOURCE_NAME AND
                    r.URL = r2.URL AND r2.ARTICLE_CONTENT IS NOT NULL
                LIMIT 1
            )
            ELSE r.ARTICLE_CONTENT
        END AS ARTICLE_CONTENT
    FROM raw as r
),

raw_clean_content as (
    SELECT  id as ID,                       
        NEWS_SOURCE_NAME,          
        NEWS_SOURCE_COUNTRY,    
        NEWS_CATEGORY,                
        AUTHOR,
        AUTHOR_EMAIL,
        AUTHOR_URL,                     
        TITLE,                      
        DESCRIPTION,          
        URL,                          
        PUBLISHEDAT,          
        ARTICLE_CONTENT,
        BIAS,
        CREATION_TIME,
        LOWER(
                LEFT(
                    REGEXP_REPLACE(ARTICLE_CONTENT, '[[:punct:]]', '', 'g'), 
                    50
                )   
            ) AS ARTICLE_CONTENT_CLEAN
    FROM BACKFILL_CONTENT
),


DROP_URL_AUTHOR AS (
    SELECT *
    FROM raw_clean_content
    WHERE AUTHOR != 'www.facebook.com'
),

-- Remove white space from author name
CLEANED_AUTHOR AS (
    SELECT 
        ID,
        NEWS_SOURCE_NAME, 
        TRIM(AUTHOR) AS AUTHOR, AUTHOR_URL, AUTHOR_EMAIL
    FROM DROP_URL_AUTHOR
),

-- Lower case the emails and remove the next line indicator '\n'
CLEANED_AUTHOR2 AS (
    SELECT 
        ID,
        NEWS_SOURCE_NAME,
        AUTHOR,
        AUTHOR_URL,
        REPLACE(LOWER(AUTHOR_EMAIL), '\n', '') as AUTHOR_EMAIL
    FROM CLEANED_AUTHOR
),

-- Articles may have the same author but may not display their email
-- or profile URL in every article they wrote, so we back fill the info.

SAME_AUTHOR_URL AS (
    SELECT 
        ca1.ID AS ID, 
        ca1.AUTHOR AS AUTHOR,
        CASE 
            WHEN ca1.AUTHOR_URL IS NULL AND ca1.AUTHOR_EMAIL IS NOT NULL THEN (
                SELECT DISTINCT ca2.AUTHOR_URL 
                FROM CLEANED_AUTHOR2 as ca2 
                WHERE ca1.AUTHOR=ca2.AUTHOR AND 
                ca1.AUTHOR_EMAIL=ca2.AUTHOR_EMAIL AND ca2.AUTHOR_URL is NOT NULL
                LIMIT 1
            )
			WHEN ca1.AUTHOR_URL IS NULL THEN (
			SELECT DISTINCT ca2.AUTHOR_URL 
                FROM CLEANED_AUTHOR2 as ca2 
                WHERE ca1.AUTHOR=ca2.AUTHOR AND 
                ca1.NEWS_SOURCE_NAME=ca2.NEWS_SOURCE_NAME AND ca2.AUTHOR_URL is NOT NULL
                LIMIT 1
			)
            ELSE ca1.AUTHOR_URL
        END as AUTHOR_URL,

        CASE 
            WHEN ca1.AUTHOR_EMAIL IS NULL AND ca1.AUTHOR_URL IS NOT NULL THEN (
                SELECT DISTINCT ca2.AUTHOR_EMAIL 
                FROM CLEANED_AUTHOR2 as ca2 
                WHERE ca1.AUTHOR=ca2.AUTHOR AND 
                ca1.AUTHOR_URL=ca2.AUTHOR_URL AND ca2.AUTHOR_EMAIL is NOT NULL
                LIMIT 1
            )
            
			WHEN ca1.AUTHOR_EMAIL IS NULL THEN (
			SELECT DISTINCT ca2.AUTHOR_EMAIL 
                FROM CLEANED_AUTHOR2 as ca2 
                WHERE ca1.AUTHOR=ca2.AUTHOR AND 
                ca1.NEWS_SOURCE_NAME=ca2.NEWS_SOURCE_NAME AND ca2.AUTHOR_EMAIL is NOT NULL
                LIMIT 1
			)
			ELSE ca1.AUTHOR_EMAIL
        END as AUTHOR_EMAIL

    FROM CLEANED_AUTHOR2 as ca1
),

-- SAME_AUTHOR_URL AS (
--     SELECT 
--         ca1.ID AS ID, 
--         ca1.AUTHOR AS AUTHOR,
--         CASE 
--             WHEN ca1.AUTHOR_URL IS NULL THEN (
--                 SELECT DISTINCT ca2.AUTHOR_URL 
--                 FROM CLEANED_AUTHOR2 as ca2 
--                 WHERE ca1.AUTHOR=ca2.AUTHOR AND 
--                 ca1.AUTHOR_EMAIL=ca2.AUTHOR_EMAIL AND ca2.AUTHOR_URL is NOT NULL
--                 LIMIT 1
--             )
--             ELSE ca1.AUTHOR_URL
--         END as AUTHOR_URL,

--         CASE 
--             WHEN ca1.AUTHOR_EMAIL IS NULL THEN (
--                 SELECT DISTINCT ca2.AUTHOR_EMAIL 
--                 FROM CLEANED_AUTHOR2 as ca2 
--                 WHERE ca1.AUTHOR=ca2.AUTHOR AND 
--                 ca1.AUTHOR_URL=ca2.AUTHOR_URL AND ca2.AUTHOR_EMAIL is NOT NULL
--                 LIMIT 1
--             )
--             ELSE ca1.AUTHOR_EMAIL
--         END as AUTHOR_EMAIL

--     FROM CLEANED_AUTHOR2 as ca1
-- ),



-- newspaper4k for some reason finds the modified-at date instead of the 
-- published-at date when searching for the published date, 
-- so we make the earliest observed publishedat date the actual published at 
-- date
fixed_publishdate AS (
    SELECT 
        ca1.ID AS ID, 
        ca1.article_CONTENT,
        (
            SELECT MIN(da.PUBLISHEDAT) 
            FROM DROP_URL_AUTHOR as da
            WHERE (ca1.title=da.title AND ca1.AUTHOR=da.AUTHOR AND ca1.NEWS_SOURCE_NAME = da.NEWS_SOURCE_NAME) OR 
                (ca1.URL=da.URL AND ca1.AUTHOR=da.AUTHOR AND ca1.NEWS_SOURCE_NAME = da.NEWS_SOURCE_NAME) OR
                (ca1.ARTICLE_CONTENT=da.ARTICLE_CONTENT AND ca1.AUTHOR=da.AUTHOR AND ca1.NEWS_SOURCE_NAME = da.NEWS_SOURCE_NAME)
            LIMIT 1 
        ) AS PUBLISHEDAT

    FROM DROP_URL_AUTHOR as ca1
),

-- Join the tables
NO_TS AS (
    SELECT 
        A.ID As ID,
        A.NEWS_SOURCE_NAME AS NEWS_SOURCE_NAME,
        A.NEWS_SOURCE_COUNTRY AS NEWS_SOURCE_COUNTRY,
        A.NEWS_CATEGORY AS NEWS_CATEGORY,
        SPLIT_PART(CA.AUTHOR, ' ', 1) AS FIRST_NAME,
        SPLIT_PART(CA.AUTHOR, ' ', 2) AS LAST_NAME,
        CA.AUTHOR_EMAIL AS AUTHOR_EMAIL,
        CA.AUTHOR_URL AS AUTHOR_URL,
        A.TITLE AS TITLE,
        A.DESCRIPTION AS DESCRIPTION,
        A.URL AS URL,
        fp.PUBLISHEDAT AS PUBLISHEDAT,
        A.ARTICLE_CONTENT AS ARTICLE_CONTENT,
        A.BIAS AS BIAS,
        A.CREATION_TIME AS CREATION_TIME,
        A.ARTICLE_CONTENT_CLEAN
    FROM DROP_URL_AUTHOR as A
    INNER JOIN SAME_AUTHOR_URL AS CA
        ON A.ID = CA.ID
    INNER JOIN fixed_publishdate AS fp
        ON fp.ID = A.ID
    WHERE A.NEWS_SOURCE_NAME != 'toronto_star' AND
        A.NEWS_SOURCE_NAME != 'cbc'
),

-- URL for an article should be less likely to change than content or title
HASHED AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['FIRST_NAME', 'LAST_NAME', 'AUTHOR_URL', 'AUTHOR_EMAIL']) }} AS AUTHOR_ID,
        {{ dbt_utils.generate_surrogate_key(['NEWS_SOURCE_NAME', 'ARTICLE_CONTENT_CLEAN']) }} AS ARTICLE_ID,
        {{ dbt_utils.generate_surrogate_key(['FIRST_NAME', 'LAST_NAME', 'AUTHOR_URL', 'AUTHOR_EMAIL', 'NEWS_SOURCE_NAME', 'TITLE', 'ARTICLE_CONTENT_CLEAN']) }} AS ARTICLE_AUTHOR_ID,
        {{ dbt_utils.generate_surrogate_key(['NEWS_SOURCE_NAME', 'NEWS_SOURCE_COUNTRY']) }} AS SOURCE_ID,
        *,
        '{{ run_started_at }}' as LOAD_TS_UTC 
        FROM NO_TS
)

SELECT * FROM HASHED