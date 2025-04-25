from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
import os
from pathlib import Path
import json
from news.get_news import parse_articles, get_news_via_links
from itertools import chain
from cosmos import (
    DbtTaskGroup, ProfileConfig, ExecutionConfig, ProjectConfig
)
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup
from news.newsairflowtasks import (
    combine_news,
    load_to_postgres,
    get_news_manual,
    check_temp_folder 
)
from news.translate_french_news import translate_french
from pendulum import now

from airflow.configuration import conf
conf.set("core", "dagbag_import_timeout", "120")

# Name of the PythonOperator for each news source x
rss_task_str = lambda x: f"Get_{x}_RSS_News"

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
CONFIG_PATH = MAIN_DIR / "config"

rssfeed_conf_path = CONFIG_PATH / "rssfeeds.json"
np_conf_path = CONFIG_PATH / f"manual_scraping_config.json"
outlet_conf_path = CONFIG_PATH / "outletinfo.json"

config_paths = [rssfeed_conf_path, outlet_conf_path, np_conf_path]

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


def load_config(path) -> list|None:
    with open(path, "r") as f:
        conf = json.load(f)
        return conf
    

start_date=days_ago(3)
with DAG(
    dag_id="News_Ingestion",
    description="",
    default_args=args,
    catchup=False,
    start_date=start_date,
    schedule_interval=timedelta(hours=3),
    max_active_tasks=4,     
    max_active_runs=1
):
    loaded_configs = [load_config(path) for 
                                          path in config_paths]
    rss_feeds_conf, link_conf, manual_news = loaded_configs

    n_articles = 10

    all_sources = [list(conf.keys()) for conf in loaded_configs]
    all_sources = list(chain.from_iterable(all_sources))
    
    with TaskGroup(group_id='Ingestion') as ingestion:
        doc_md = """
        Check if there is already an existing csv file from a failed loading.
        If there is a file present, the load_to_postgres task is ran and
        the extraction is skipped.
        """
        check_file = BranchPythonOperator(
            task_id="check_for_file",
            python_callable=check_temp_folder,
            op_args=[all_sources],
            doc_md = doc_md
        )

        with TaskGroup("Extract_News") as extract:
            # Gather news from each news source in paralell
            # We store tasks that rely on selenium in a separate list
            # so that they can run sequentially.
            tasks = []
            selenium_tasks = []
            # News scraped by RSS feed links
            for source in rss_feeds_conf:
                source_dict = rss_feeds_conf[source]
                get_news = PythonOperator(
                    task_id=rss_task_str(source),
                    python_callable=parse_articles,
                    op_args=[source_dict, n_articles],
                    execution_timeout=timedelta(minutes=15)
                )
                tasks.append(get_news)

            # News scraped by finding links in main page
            for source in link_conf:
                if source not in rss_feeds_conf:
                    get_link_news = PythonOperator(
                        task_id=rss_task_str(source),
                        python_callable=get_news_via_links,
                        op_args=[link_conf[source], source],
                        op_kwargs={"n_articles": n_articles},
                        execution_timeout=timedelta(minutes=15)
                    )
                    
                    if link_conf[source].get("use_selenium", False):
                        selenium_tasks.append(get_link_news)
                    else:
                        tasks.append(get_link_news)

            # News scraped manually (newspaper3k/4k does not work)
            for source in manual_news:
                if source not in rss_feeds_conf:
                    np_news_task = PythonOperator(
                        task_id=rss_task_str(source),
                        python_callable=get_news_manual,
                        op_args=[manual_news[source], n_articles],
                        execution_timeout=timedelta(minutes=15)
                    )
                    tasks.append(np_news_task)

        
        combine = PythonOperator(
            task_id="Combine_News",
            python_callable=combine_news,
            op_kwargs={"sources": all_sources},
            trigger_rule=TriggerRule.ALL_DONE
        )

        load = PythonOperator(
            task_id="Load_to_Postgres",
            python_callable=load_to_postgres
        )

        for i, item in enumerate(selenium_tasks[1:]):
            selenium_tasks[i] >> item

        check_file >> selenium_tasks[0]
        selenium_tasks[-1] >> combine
        check_file >> tasks
        tasks >> combine >> load
        check_file >> load

    with TaskGroup(group_id='Python_Transformations') as pytransform:
        translate = PythonOperator(
            task_id="Translate_French_Articles",
            python_callable=translate_french
        )
        translate

    transform = DbtTaskGroup(
        group_id="Transform_Data",
        project_config=ProjectConfig(DBT_PROJ_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
    )


ingestion >> pytransform >> transform
