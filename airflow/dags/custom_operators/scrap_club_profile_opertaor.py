from modules.log import log
from modules.retry import RetryOnException as retry
from modules.club import ClubProfileProducer
from modules.util import UrlVaildator

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


@log
class ScrapClubProfileOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self, 
        http_header,
        url, 
        **kwargs):
        super().__init__(**kwargs)
        self.http_header=http_header
        self.url = url

    @retry(5)
    def execute(self, context):
        url_vaildator = UrlVaildator(self.http_header)
        url_content = url_vaildator.verify_url(self.url)

        club_profile_producer = ClubProfileProducer()
        club_profiles = club_profile_producer.produce_club_info(url_content)

        return club_profiles
