"""
Python script which contains all of the tasks for the ingest_news Airflow DAG
"""
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.state import State
import datetime
import os
from news.get_news_manual import ManualNewsScraper, AuthorFinder
from itertools import chain
import pandas as pd
from pathlib import Path
import logging
from news.log_setup import setup_logging
from news.utils import get_news_info_schema
import numpy as np

setup_logging()
logger = logging.getLogger('ingest_news')

rss_task_str = lambda x: f"Get_{x}_RSS_News"
MAIN_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent.parent

SCHEMA = get_news_info_schema(
    MAIN_DIR / "config/scraper_output_schema.json"
)

def combine_news(ti, sources:list) -> list:
    """ Pulls the results for the extraction tasks and combines them
    into a list
    """
    # Make list of the PythonOperator task ids
    # 'Ingestion' is the task group ID 
    task_ids = ["Ingestion.Extract_News." + rss_task_str(src) for src in sources]
    
    tasks_to_pull = []
    for task in task_ids:
        upstream_ti = ti.get_dagrun().get_task_instance(task)
        if upstream_ti and upstream_ti.state == State.SUCCESS:
            tasks_to_pull.append(task)

    logger.debug("tasks_to_pull:", tasks_to_pull)

    # Pull the list of news data
    data = list(
        ti.xcom_pull(task_ids=tasks_to_pull)
    ) # tuple of lists containing dicts
    
    # Convert the tuple of lists into a list of dicts containing 
    # all scraped news
    data_flattened = list(chain.from_iterable(data))
    
    return data_flattened


def load_to_postgres(ti):
    os.makedirs(MAIN_DIR / "temp", exist_ok=True)
    temp_file_path = MAIN_DIR / "temp/tmp_news.csv"

    if 'tmp_news.csv' not in os.listdir(MAIN_DIR / "temp"):
        news = ti.xcom_pull(task_ids="Ingestion.Combine_News")
        expected_keys = list(SCHEMA.keys())
        print(expected_keys)
        
        df = pd.DataFrame(news)
        df = df[expected_keys] # make sure columns are in order
        df['ingest_ts_utc'] = datetime.datetime.now(datetime.timezone.utc)
        
        df_sample = df.drop(["content"], axis=1).to_string()
        logger.debug(f"dataframe:\n {df_sample}")

        df.to_csv(temp_file_path, index=False, header=False)
    
    pg_hook = PostgresHook(postgres_conn_id="postgres")

    try:
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        with open(temp_file_path, 'r') as f:
            cursor.copy_expert("""
            COPY raw_news (article_content, author, 
                description, publishedat, title, category, url, 
                source_name, source_country, bias, author_email, author_url,
                language, ingest_ts)   
            FROM STDIN WITH CSV
            """, f)
        conn.commit()
        os.remove(temp_file_path)
        logger.info("Successfully loaded data")
    except Exception as e:
        logger.error("Failed to load data to postgres")
        raise
    finally:
        cursor.close()
        conn.close()
    

def get_news_manual(settings, n_articles):
    author_finder = AuthorFinder(
        base_url=settings['base_url'], 
        email_search_settings = settings.get("email_search_settings")
    )
    scraper = ManualNewsScraper(settings, author_parser=author_finder)
    return scraper.parse_articles(n_articles=n_articles)


def check_temp_folder(sources):
    os.makedirs(MAIN_DIR / "temp", exist_ok=True)
    if 'tmp_news.csv' not in os.listdir(MAIN_DIR / "temp"):
        task_ids = ["Ingestion.Extract_News." + rss_task_str(src) for src in sources]
        return task_ids
    else:
        return ["Ingestion.Load_to_Postgres"]