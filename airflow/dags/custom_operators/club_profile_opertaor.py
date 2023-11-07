from log import log
from retry import RetryOnException as retry
from club import (
    ClubProfileExporter,
    ClubProfileProducer
)

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


@log
class ClubProfileOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            bootstrap_servers,
            topic,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

    @retry(5)
    def execute(self, context):
        club_profile_producer = ClubProfileProducer()
        club_info = club_profile_producer.produce_club_info()

        with ClubProfileExporter(self.bootstrap_servers) as exporter:
            try:
                exporter.export_club_info(
                    self.topic,
                    club_info
                )
            except Exception as err:
                raise err
