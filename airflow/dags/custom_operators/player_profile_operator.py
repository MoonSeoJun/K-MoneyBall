import logging
from modules.log import log
from modules.retry import RetryOnException as retry
from modules.player import (
    PlayerProfileProducer,
    PlayerProfileExporter
)
from modules.util import (
    UrlVaildator
)

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaConsumer

import time

@log
class PlayerProfileOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            http_header,
            bootstrap_servers,
            topic,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.http_hader = http_header
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

        self.consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)

    @retry(5)
    def execute(self, context):
        logging.info("Execute Start")
        for message in self.consumer:
            logging.info(message.value)
            continue
            player_exporter = PlayerProfileExporter(self.bootstrap_servers)
            
            url_vaildator = UrlVaildator(self.http_hader)
            url_content = url_vaildator.verify_url(self.club_url)

            player_profile_producer = PlayerProfileProducer()

            player_urls = player_profile_producer.get_player_urls(url_content)

            for player_url in player_urls:
                player_url_content = url_vaildator.verify_url(player_url)
                player_profile = player_profile_producer.get_player_profile(player_url, player_url_content)

                time.sleep(1)

                try:
                    player_exporter.export_player_profile(
                        self.topic, 
                        player_profile
                    )
                except Exception as err:
                    raise err