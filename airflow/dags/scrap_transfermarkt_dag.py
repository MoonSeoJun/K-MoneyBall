from urllib.parse import urlparse
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.baseoperator import chain
from kafka import KafkaConsumer

from dags_config import Config as config
from custom_operators import (
    ClubProfileOperator,
    PlayerProfileOperator
)


def dummy_callable(action):
    return f"{datetime.now()}: {action} scrapping TransferMarkt!"

def export_club_profile(league, config, dag):
    return ClubProfileOperator(
        task_id=f"{league['title']}_club_profile_exporting",
        url=league['url'],
        http_header=config.REQUEST_HEADERS,
        bootstrap_servers=config.BOOTSTRAP_SERVERS,
        topic=config.CLUB_TOPIC,
        dag=dag
    )

def export_player_profile(league, config, dag):
    return PlayerProfileOperator(
        task_id=f"{league['title']}_player_profile_exporting",
        url=league['url'],
        http_header=config.REQUEST_HEADERS,
        bootstrap_servers=config.BOOTSTRAP_SERVERS,
        topic=config.CLUB_TOPIC,
        dag=dag
    )

with DAG(
    dag_id="scrap_transfermarkt_dag",
    description=f"Scrape latest club and player profiles",
    schedule_interval="@hourly",
    start_date=datetime(2020, 1, 1),
    catchup=False,
    is_paused_upon_creation=False
) as dag:

    start = PythonOperator(
        task_id="starting_pipeline",
        python_callable=dummy_callable,
        op_kwargs={"action": "starting"},
        dag=dag
    )

    scrapping_club_profile = [
        export_club_profile(league, config, dag)
        for league in config.KLEAGUE_URLS
    ]

    finish = PythonOperator(
        task_id="finishing_pipeline",
        python_callable=dummy_callable,
        op_kwargs={"action": "finishing"},
        dag=dag
    )

    finish_club_scrapping = PythonOperator(
        task_id="finishing_club_scrapping",
        python_callable=dummy_callable,
        op_kwargs={"action": "finishing"},
        dag=dag
    )

    # chain(start, [scrapping_player_profile], scrapping_club_profile, finish)

    start >> scrapping_club_profile >> finish