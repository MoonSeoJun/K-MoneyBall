import requests
from requests.exceptions import ConnectionError

import util.configs as configs

class UrlVaildator:
    def __init__(self) -> None:
        pass
    
    def verify_url(self, target_url):
        try:
            req = requests.get(target_url, headers=configs.REQUEST_HEADERS)

            assert req.status_code == requests.codes.ok, f"{target_url} is invalid url by {req.status_code}"

            return req.text
        except ConnectionError as e:
            raise e