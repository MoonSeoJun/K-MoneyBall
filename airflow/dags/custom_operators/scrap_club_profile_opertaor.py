from modules.log import log
from modules.retry import RetryOnException as retry
from modules.club import ClubProfileProducer, ClubProfileExporter
from modules.util import UrlVaildator

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


@log
class ScrapClubProfileOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self, 
        http_header,
        bootstrap_servers,
        topic,
        url, 
        **kwargs):
        super().__init__(**kwargs)
        self.http_header=http_header
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.url = url

    @retry(5)
    def execute(self, context):
        url_vaildator = UrlVaildator(self.http_header)
        url_content = url_vaildator.verify_url(self.url)

        club_profile_producer = ClubProfileProducer()
        club_profiles = club_profile_producer.produce_club_info(url_content)

        for club_profile in club_profiles:
            url_content = url_vaildator.verify_url(club_profile['url'])
            current_club_title = club_profile_producer.produce_current_club_title(url_content)
            club_profiles[club_profiles.index(club_profile)]['club_name'] = current_club_title

        with ClubProfileExporter(self.bootstrap_servers) as exporter:
            try:
                exporter.export_club_profile(
                    topic=self.topic,
                    club_profiles=club_profiles
                )
            except Exception as err:
                raise err

        return club_profiles
