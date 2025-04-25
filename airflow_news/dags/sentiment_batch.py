from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.state import State
from datetime import timedelta
import datetime
import os
from pathlib import Path

from itertools import chain
import pandas as pd
from cosmos import (
    DbtTaskGroup, ProfileConfig, ExecutionConfig, ProjectConfig
)
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup
from cosmos.profiles import PostgresUserPasswordProfileMapping
from news.sentiment_batch_job_tasks import (
    pull_daily_data, clean_data, get_sentiment, check_unloaded_batch,
    get_sentiment_batch_results, load_sentiment
)


args = {
    'owner': "Garett",
    'depends_on_past': False,
    "start_date": days_ago(31),
    "email": "gkaube@outlook.com",
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

MAIN_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent

DBT_PROJ_PATH = "/opt/airflow/dbtnews"
DBT_PROFILE_PATH = "/opt/airflow/dbtnews/profiles.yml"
DBT_EXECUTABLE_PATH = "/home/airflow/.local/bin/dbt"

profile_config = ProfileConfig(
    profile_name="news",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILE_PATH
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

with DAG(
    dag_id="Sentiment_Analysis",
    description="",
    default_args=args,
    catchup=False,
    start_date=datetime.datetime.today(),
    schedule='15 00 * * *',
):
    
    # Number of days ago for published articles to run the sentiment analyzer on 
    n_days_back = 2
    
    file_check = BranchPythonOperator(
        task_id="Check_for_sentiment_csv",
        python_callable=check_unloaded_batch
    )

    fetch_news = PythonOperator(
        task_id="Fetch_news_from_Postgres",
        python_callable=pull_daily_data,
        op_kwargs={"n_days":n_days_back}
    )

    clean = PythonOperator(
        task_id="Clean_Data",
        python_callable=clean_data
    )

    sentiment = PythonOperator(
        task_id="Analyze_Sentiment",
        python_callable=get_sentiment,
        execution_timeout=timedelta(minutes=80),
        retries=0
    )

    get_sentiment_batch = PythonOperator(
        task_id="Retreive_Sentiment_Batch",
        python_callable=get_sentiment_batch_results
    )
    load = PythonOperator(
        task_id="Load_to_Postgres",
        python_callable=load_sentiment
    )

    # # The transform method is ran to ensure relation ships between 
    # # article table and sentiment table
    transform = DbtTaskGroup(
        group_id="Transform_Data",
        project_config=ProjectConfig(DBT_PROJ_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
    )

file_check >> fetch_news >> clean >> sentiment >> get_sentiment_batch >> load >> transform
file_check >> load