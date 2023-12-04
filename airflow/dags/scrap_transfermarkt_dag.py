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

from modules.club import ClubProfileExporter
from modules.player import PlayerProfileExporter

import logging
import re


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
    def export_data(bootstrap_servers, topic, data_list):
        data = combine_url_list(data_list)

        if topic == config.CLUB_TOPIC:
            with ClubProfileExporter(bootstrap_servers) as exporter:
                try:
                    exporter.export_club_profile(
                        topic,
                        data
                    )
                    return data
                except Exception as err:
                    raise err
        else:
            with PlayerProfileExporter(bootstrap_servers) as exporter:
                try:
                    exporter.export_player_profile(
                        topic,
                        data
                    )
                    return data
                except Exception as err:
                    raise err
                
    @task
    def verify_data_by_mongo(mongo_host, mongo_db, mongo_collection, data_list):
        datas = combine_url_list(data_list)

        myclient = pymongo.MongoClient(mongo_host)
        mydb = myclient.get_database(mongo_db)
        colc = mydb.get_collection(mongo_collection)

        if mongo_collection == config.MONGO_COLLECTION_CLUBS:
            for data in datas:
                if colc.find_one({"club_name" : data["club_name"]}):
                    result = colc.find_one_and_update(
                        {"club_name" : data["club_name"]}, 
                        update={"$set" : data},
                        return_document=ReturnDocument.AFTER)
                else:
                    result = colc.insert_one(document=data)
                logging.info(result)
        else:
            for data in datas:
                date_of_birth = data["Date of birth/Age"][0:-5]
                reg_birth = re.compile(f"^{date_of_birth}")

                if colc.find_one({
                    'Date of birth/Age' : {"$regex" : reg_birth},
                    'shirt_name' : data["shirt_name"]}):
                    result = colc.find_one_and_update(
                        {'Date of birth/Age' : {"$regex" : reg_birth},
                        'shirt_name' : data["shirt_name"]}, 
                        update={"$set" : data},
                        return_document=ReturnDocument.AFTER)
                else:
                    result = colc.insert_one(document=data)
                logging.info(result)
        
        myclient.close()

        return datas
    
    @task
    def extract_club_url(url_list):
        club_urls = [club['club_url'] for club in url_list]
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
    ).expand(
        url=config.KLEAGUE_URLS,
    )
    

    verify_club_mongo_task = verify_data_by_mongo(mongo_host=config.MONGO_HOST,
                                                  mongo_db=config.MONGO_DB,
                                                  mongo_collection=config.MONGO_COLLECTION_CLUBS,
                                                  data_list=scrap_club_profile_task.output)
    
    extract_club_url = extract_club_url(url_list=verify_club_mongo_task)

    scrap_player_profile_task = ScrapPlayerProfileOperator.partial(
        task_id="scrap_player_profile_task",
        http_header=config.REQUEST_HEADERS,
    ).expand(
        url=extract_club_url
    )

    verify_player_mongo_task = verify_data_by_mongo(mongo_host=config.MONGO_HOST,
                                                  mongo_db=config.MONGO_DB,
                                                  mongo_collection=config.MONGO_COLLECTION_PLAYERS,
                                                  data_list=scrap_player_profile_task.output)

    finish = PythonOperator(
        task_id="finishing_pipeline",
        python_callable=dummy_callable,
        op_kwargs={"action": "finishing"},
        dag=dag
    )

    start >> scrap_club_profile_task >> verify_club_mongo_task >> extract_club_url >> scrap_player_profile_task
    scrap_player_profile_task >> verify_player_mongo_task >> finish
