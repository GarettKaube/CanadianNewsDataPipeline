""" This DAG is for manually loading a processed sentiment batch
"""

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow import DAG
from datetime import timedelta
import datetime
import os
from pathlib import Path
from cosmos import (
    DbtTaskGroup, ProfileConfig, ExecutionConfig, ProjectConfig
)
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup
from news.sentiment_batch_job_tasks import (
    get_sentiment_batch_results,
    load_sentiment
)


args = {
    'owner': "Airflow",
    'depends_on_past': False,
    "start_date": days_ago(31),
    "email": "",
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
    dag_id="load_sentiment",
    description="",
    default_args=args,
    catchup=False,
    schedule='15 00 * * *',
    params={"batch_file_name": "batch_file_name"}
) as dag:
    
    get_batch = PythonOperator(
        task_id = "Get_Sentiment_Batch",
        python_callable=get_sentiment_batch_results,
        provide_context=True
    )
    
    load = PythonOperator(
        task_id="Load_to_Postgres",
        python_callable=load_sentiment
    )

    # The transform method is ran to ensure relation ships between 
    # article table and sentiment table
    transform = DbtTaskGroup(
        group_id="Transform_Data",
        project_config=ProjectConfig(DBT_PROJ_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
    )

get_batch >> load >> transform