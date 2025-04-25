{{  config(
        materialized="view",
        sort ='publishedat'
    )
}}

WITH raw as (
    SELECT  id as ID,                       
        source_name::VARCHAR as NEWS_SOURCE_NAME,          
        source_country::VARCHAR as NEWS_SOURCE_COUNTRY,    
        category::VARCHAR as NEWS_CATEGORY,                
        author::VARCHAR as AUTHOR,
        author_email::VARCHAR as AUTHOR_EMAIL,
        author_url::VARCHAR as AUTHOR_URL,                     
        title::VARCHAR as TITLE,                      
        description::VARCHAR as DESCRIPTION,          
        url::VARCHAR as URL,                          
        publishedat::TIMESTAMP as PUBLISHEDAT,          
        article_content::TEXT as ARTICLE_CONTENT,
        bias AS BIAS,
        ingest_ts::TIMESTAMP AS CREATION_TIME  
    FROM {{ source("news_source_table", "raw_news") }}
)

SELECT * FROM raw