from modules.log import log
from modules.retry import RetryOnException as retry
from modules.club import (
    ClubProfileExporter,
    ClubProfileProducer
)
from modules.util import (
    UrlVaildator
)

from dags_config import Config as config

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


@log
class ClubProfileOperator(BaseOperator):

    @apply_defaults
    def __init__(self, url, **kwargs):
        super().__init__(**kwargs)
        self.url = url

        self.http_header=config.REQUEST_HEADERS,
        self.bootstrap_servers=config.BOOTSTRAP_SERVERS,
        self.topic=config.CLUB_TOPIC

    @retry(5)
    def execute(self, context):
        url_vaildator = UrlVaildator(self.http_header[0])
        url_content = url_vaildator.verify_url(self.url)

        club_profile_producer = ClubProfileProducer()
        club_info = club_profile_producer.produce_club_info(url_content)

        with ClubProfileExporter(self.bootstrap_servers[0]) as exporter:
            try:
                exporter.export_club_info(
                    self.topic,
                    club_info
                )
                return club_info
            except Exception as err:
                raise err
