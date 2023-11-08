from log import log
from retry import RetryOnException as retry
from club import (
    ClubProfileExporter,
    ClubProfileProducer
)
from util import (
    UrlVaildator
)

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


@log
class ClubProfileOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            url,
            http_header,
            bootstrap_servers,
            topic,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url
        self.http_hader = http_header
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

    @retry(5)
    def execute(self, context):
        url_vaildator = UrlVaildator(self.http_hader)
        url_content = url_vaildator.verify_url(self.url)

        club_profile_producer = ClubProfileProducer()
        club_info = club_profile_producer.produce_club_info(url_content)

        with ClubProfileExporter(self.bootstrap_servers) as exporter:
            try:
                exporter.export_club_info(
                    self.topic,
                    club_info
                )
            except Exception as err:
                raise err
