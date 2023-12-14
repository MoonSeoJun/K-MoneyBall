import pymongo
from pymongo import ReturnDocument
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task

from dags_config import Config as config
from custom_operators import (
    ScrapClubProfileOperator,
    ScrapPlayerProfileOperator,
)


def dummy_callable(action):
    return f"{datetime.now()}: {action}"

def combine_url_list(url_list):
    urls = sum(url_list, [])
    return urls

with DAG(
    dag_id="scrap_transfermarkt_dag",
    description=f"Scrape latest club and player profiles",
    schedule_interval="@hourly",
    start_date=datetime(2020, 1, 1),
    catchup=False,
    is_paused_upon_creation=False
) as dag:
    @task
    def extract_club_url(url_list):
        urls = combine_url_list(url_list)
        club_urls = [club['url'] for club in urls]
        return club_urls

    start = PythonOperator(
        task_id="starting_pipeline",
        python_callable=dummy_callable,
        op_kwargs={"action": "starting Pipeline"},
        dag=dag
    )
    

    scrap_club_profile_task = ScrapClubProfileOperator.partial(
        task_id="scrap_club_profile_task",
        http_header=config.REQUEST_HEADERS,
        bootstrap_servers=config.BOOTSTRAP_SERVERS,
        topic=config.CLUB_TOPIC
    ).expand(
        url=config.KLEAGUE_URLS,
    )
    
    extract_club_url = extract_club_url(url_list=scrap_club_profile_task.output)

    scrap_player_profile_task = ScrapPlayerProfileOperator.partial(
        task_id="scrap_player_profile_task",
        http_header=config.REQUEST_HEADERS,
        bootstrap_servers=config.BOOTSTRAP_SERVERS,
        topic=config.PLAYER_TOPIC
    ).expand(
        url=extract_club_url
    )

    finish = PythonOperator(
        task_id="finishing_pipeline",
        python_callable=dummy_callable,
        op_kwargs={"action": "finishing"},
        dag=dag
    )

    start >> scrap_club_profile_task >> extract_club_url >> scrap_player_profile_task >> finish
