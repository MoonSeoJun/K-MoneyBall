import requests
from requests.exceptions import ConnectionError


class UrlVaildator:
    def __init__(self, header) -> None:
        self._http_header = header
    
    def verify_url(self, target_url):
        try:
            req = requests.get(target_url, headers=self._http_header)

            assert req.status_code == requests.codes.ok, f"{target_url} is invalid url by {req.status_code}"

            return req.text
        except ConnectionError as e:
            raise e