"""
Simple script that contains the function that uses google translate 
to translate french news articles in Postgres to English.
"""
import sqlalchemy as sa
from sqlalchemy import update
import pandas as pd
import asyncio
from googletrans import Translator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from news.log_setup import setup_logging
import logging

setup_logging(log_file_path='/opt/airflow/logs/translate.log')
logger = logging.getLogger("translate")

def translate_french():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    engine = pg_hook.get_sqlalchemy_engine()

    meta_data = sa.MetaData()
    table = sa.Table(
        "raw_news", 
        meta_data, 
        autoload_with=engine, 
        schema="public"
    )

    # Get all rows that have language column = french
    with engine.connect() as conn:
        query = sa.select(table)\
            .where(table.c.language=='french')
        results = conn.execute(query).fetchall()
    
    df = pd.DataFrame(results)

    logger.info(f"Number of articles to translate: {df.shape[0]}")

    translator = Translator()
    async def translate_french(x):
        result = await translator.translate(x , src='fr', dest='en')
        result = result.text
        return result

    # Translate each row and update the language column to english
    async def process_data(df):    
        with engine.connect() as conn:
            for i, row in df.iterrows():
                print(i)
                translated_content = await translate_french(row['article_content'])
                translated_title = await translate_french(row['title'])
                stmt = (
                    update(table)
                    .where(table.c.id == row['id'])
                    .values(
                        title=translated_title, 
                        article_content=translated_content,
                        language='english'
                    ) 
                )
                conn.execute(stmt)

    asyncio.run(process_data(df))  
