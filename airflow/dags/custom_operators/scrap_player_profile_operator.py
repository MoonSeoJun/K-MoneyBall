import logging
from modules.log import log
from modules.retry import RetryOnException as retry
from modules.player import PlayerProfileProducer, PlayerProfileExporter
from modules.util import UrlVaildator

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

import time

@log
class ScrapPlayerProfileOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            http_header,
            bootstrap_servers,
            topic,
            url,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.http_header = http_header
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.club_url = url

    @retry(5)
    def execute(self, context):            
        url_vaildator = UrlVaildator(self.http_header)
        url_content = url_vaildator.verify_url(self.club_url)

        player_profile_producer = PlayerProfileProducer()

        player_urls = player_profile_producer.get_player_urls(url_content)

        player_profiles = []

        for player_url in player_urls:
            player_url_content = url_vaildator.verify_url(player_url)
            player_profile = player_profile_producer.get_player_profile(player_url, player_url_content)

            logging.info(f"player_profile : {player_profile}")

            player_profiles.append(player_profile)

        with PlayerProfileExporter(self.bootstrap_servers) as exporter:
            try:
                exporter.export_player_profile(
                    topic=self.topic,
                    player_profiles=player_profiles
                )
            except Exception as err:
                raise err
