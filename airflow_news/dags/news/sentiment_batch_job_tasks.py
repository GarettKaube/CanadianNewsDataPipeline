import os
import pandas as pd
import datetime
from openai import OpenAI
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from news.log_setup import setup_logging
import logging
from pathlib import Path
from airflow.models import Variable
from news.PydanticModels import SentimentDC
import time
from openai.lib._parsing._completions import type_to_response_format_param

response_json_schema = type_to_response_format_param(SentimentDC)

open_api_key = Variable.get("OpenAIKey")

openai_client = OpenAI(
    api_key=open_api_key
)

MAIN_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent.parent

db_address = os.getenv("POSTGRES_ADRESS")

setup_logging()
logger = logging.getLogger("sentiment_analysis")

def check_unloaded_batch():
    if 'tmp_sentiment.csv' not in os.listdir(MAIN_DIR / "temp"):
        return "Fetch_news_from_Postgres"
    else:
        return "Load_to_Postgres"

def pull_daily_data(n_days:int=1) -> pd.DataFrame | None:
    # If 'tmp_sentiment.csv' is present, that means there is a batch that was 
    # not loaded to postgres
    pg_hook = PostgresHook(postgres_conn_id="postgres")
    yesterday = pd.Timestamp(datetime.datetime.now()) - pd.DateOffset(days=n_days)
    yesterday = yesterday.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"yesturday: {yesterday}")
    df = None

    try:
        query = f"""
            SELECT 
                article_content, 
                publishedat, 
                url, 
                article_id
            FROM "STAGE_DATAMART".articles
            WHERE (article_content LIKE '%Carney%' OR 
                article_content LIKE '%Poilievre%') AND 
                publishedat >= '{yesterday}' AND
                LENGTH(article_content) >= 20
        """
        logger.debug(f"Query:\n{query}")

        df = pg_hook.get_pandas_df(query)
        logger.debug(f"returned df: {df}")
    except Exception as e:
        logger.exception("Failed to get df")
    return df


    

    # engine = sa.create_engine(db_address)
    # meta_data = sa.MetaData()
    # table = sa.Table("articles", meta_data, autoload_with=engine, schema="STAGE_REF")
    # yesterday = pd.Timestamp(datetime.datetime.now()) - pd.DateOffset(days=1)
    # with engine.connect() as conn:
    #     query = sa.select(table.c.article_content, table.c.publishedat, table.c.url, table.c.article_id)\
    #         .where(table.c.publishedat >= yesterday)\
    #         .where(table.c.article_content.ilike("%Carney%") | table.c.article_content.ilike("%Poilievre%"))
        
        # result = conn.execute(query).fetchall()
        # df = pd.DataFrame(result, columns=result[0].keys())
    

def clean_data(ti):
    df:pd.DataFrame = ti.xcom_pull(task_ids="Fetch_news_from_Postgres")
    df['article_content'] = df['article_content'].str.replace("\\n", "")

    mailto_patern = r"[\w\.-]+@[\w\-]+\.[a-zA-Z]{2,6}"

    # Remove trending now
    df['article_content'] = df['article_content'].str\
        .split("trending now").str[0]
    
    df['article_content'] = df['article_content'].str\
        .split("Trending Now").str[0]
    
    # Remove emails
    df['article_content'] = df['article_content'].str.replace(
        mailto_patern,"", regex=True
    )

    # Remove URL's
    pattern = r"www\.[a-zA-Z0-9\-]+\.[\w]{2,6}"
    df['article_content'] = df['article_content'].str.replace(
        pattern, "", regex=True
    )

    return df



def get_sentiment_realtime(ti):
    """
    Takes the dataframe from the 'Clean_Data' task and creates a batch
    job for openai to calculate sentiment for the articles in a batch
    """
    batch_path = MAIN_DIR / "temp"/ "request.jsonl"

    # If 'tmp_sentiment.csv' is present, that means there is a batch that was 
    # not loaded to postgres
    if check_unloaded_batch():
        df = ti.xcom_pull(task_ids="Clean_Data")

        model = "gpt-4o-mini"
        sentiment = []
        strings = []

        instructions = """
            You are a sentiment analyzer.
            Instructions:
            - If the article mentions either Mark Carney or Pierre Poilievre, return a sentiment score from 0.00 to 1.00 (2 decimal points) for each, representing the positivity of the sentiment toward that person.
            - If Mark Carney or Pierre Poilievre is not mentioned, return their score as "N/A".
            - Use the full name as the key (e.g., "sentiment_mark" for Mark Carney, "sentiment_poilievre" for Pierre Poilievre).
            - Return only the structured output in this exact format:
            {
            "sentiment_mark": "<score or N/A>",
            "sentiment_poilievre": "<score or N/A>"
            }

            - Sort fields alphabetically by politician's last name.
        """

        # Create the prompts for each article in the batch
        for i, row in df.iterrows():
            logger.info(f"URL: {row['url']}")
            text = row['article_content']
            msg = {
                "custom_id": f"{row['article_id']}", 
                "method": "POST", 
                "url": "/v1/chat/completions", 
                "body": {
                    "model": f"{model}", 
                    "messages": [
                        {
                            "role": "system", 
                            "content":instructions
                        },
                        {
                             "role": "user", 
                            "content": text
                        }
                    ],
                    "response_format": response_json_schema
                }
            }

def get_sentiment(ti) -> list[dict, str|float|None]:
    """
    Takes the dataframe from the 'Clean_Data' task and creates a batch
    job for openai to calculate sentiment for the articles in a batch
    """
    batch_path = MAIN_DIR / "temp"/ "request.jsonl"

    # If 'tmp_sentiment.csv' is present, that means there is a batch that was 
    # not loaded to postgres
    df = ti.xcom_pull(task_ids="Clean_Data")

    model = "gpt-4o-mini"
    sentiment = []
    strings = []

    instructions = """
        You are a sentiment analyzer.
        Instructions:
        - If the article mentions either Mark Carney or Pierre Poilievre, return a sentiment score from 0.00 to 1.00 (2 decimal points) for each, representing the positivity of the sentiment toward that person.
        - If Mark Carney or Pierre Poilievre is not mentioned, return their score as None.
        - Use the full name as the key (e.g., "sentiment_mark" for Mark Carney, "sentiment_poilievre" for Pierre Poilievre).
        - Return only the structured output in this exact format:
        {
        "sentiment_mark": "<score or N/A>",
        "sentiment_poilievre": "<score or N/A>"
        }

        - Sort fields alphabetically by politician's last name.
    """

    # Create the prompts for each article in the batch
    for i, row in df.iterrows():
        logger.info(f"URL: {row['url']}")
        text = row['article_content']
        msg = {
            "custom_id": f"{row['article_id']}", 
            "method": "POST", 
            "url": "/v1/chat/completions", 
            "body": {
                "model": f"{model}", 
                "messages": [
                    {
                        "role": "system", 
                        "content":instructions
                    },
                    {
                            "role": "user", 
                        "content": text
                    }
                ],
                "response_format": response_json_schema
            }
        }
        strings.append(msg)
    
    # Write the prompts to jsonl
    with open(batch_path, "w") as f:
        to_write = [json.dumps(string) + "\n" for string in strings]
        f.writelines(to_write)    

    # Create the batch job
    batch_input_file = openai_client.files.create(
        file=open(batch_path, "rb"),
        purpose="batch"
    )

    batch_input_file_id = batch_input_file.id
    batch_request = openai_client.batches.create(
        input_file_id=batch_input_file_id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={
            "description": "Sentiment Analysis"
        }
    )

    # Wait untill the job is done/failed
    batch = openai_client.batches.retrieve(batch_request.id)
    status = batch.status
    while status not in ['failed', 'completed', 'expired', 'cancelled']:
        batch = openai_client.batches.retrieve(batch_request.id)
        status = batch.status
        logger.info(f"Batch job status: {status}")
        time.sleep(20)

    if status == "completed":
        batch = openai_client.batches.retrieve(batch_request.id)
        return batch.output_file_id
    else:
        raise RuntimeError("Sentiment job failed.")
        


def get_sentiment_batch_results(ti):
    sentiment = []

    output_file_name = ti.xcom_pull(task_ids="Analyze_Sentiment")
    logger.info(f"Output file name: {output_file_name}")

    file_response = openai_client.files.content(output_file_name)

    lines = file_response.text.splitlines()

    for line in lines:
        article_id = json.loads(line)['custom_id']
        content = (json.loads(line)["response"]['body']['choices'][0]
            ['message']
            ['content']
        )
        
        content = json.loads(content)
        logger.debug(f"Response content:\n {content}")

        data = {
            "article_id": article_id,
            "sentiment_mark": content.get("sentiment_mark"),
            "sentiment_poilievre": content.get("sentiment_poilievre"),
        }
        logger.debug(f"data: :\n {data}")

        sentiment.append(data)
    
    logger.info("Done")
    return sentiment


def load_sentiment(ti):
    temp_file_path = MAIN_DIR / "temp/tmp_sentiment.csv"
    os.makedirs(MAIN_DIR / "temp", exist_ok=True)

    if 'tmp_sentiment.csv' not in os.listdir(MAIN_DIR / "temp"):
        sentiment = ti.xcom_pull(task_ids = "Get_Sentiment_Batch")

        sent_df = pd.DataFrame(sentiment)
        sent_df['ingest_ts_utc'] = datetime.datetime.now(datetime.timezone.utc)
        sent_df = sent_df.replace({"N/A": None})
        sent_df.to_csv(temp_file_path, index=False, header=False)

    pg_hook = PostgresHook(postgres_conn_id="postgres")

    try:
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        with open(temp_file_path, 'r') as f:
            cursor.copy_expert("""
            COPY sentiment_raw (article_id, sentiment_mark, 
                               sentiment_poilievre, ingest_ts)   
            FROM STDIN WITH CSV
            """, f)
        conn.commit()
    except Exception as e:
        print(e)
        logger.exception("Failed to load data")
    finally:
        cursor.close()
        conn.close()
        os.remove(temp_file_path)
    