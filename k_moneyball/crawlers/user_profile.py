import requests
from bs4 import BeautifulSoup

from typing import List

from configs import REQUEST_HEADERS, TRANSFER_MARKT_ROOT_URL

class UserProfile:
    def __init__(self) -> None:
        self.national_num_kr = 87
        self.root_url = f"{TRANSFER_MARKT_ROOT_URL}/wettbewerbe/national/wettbewerbe/87"
        return

    def get_league_urls(self) -> List:
        league_urls = []

        req = requests.get(self.root_url, headers=REQUEST_HEADERS)

        if req.status_code == requests.codes.ok:
            soup = BeautifulSoup(req.text, "lxml")

            league_cup_table = soup.find("div", {"class" : "responsive-table"})
            league_cups_tags = league_cup_table.find_all("img", {"class" : "continental-league-emblem"})

            for tag in league_cups_tags:
                league = soup.find("a", {"title" : tag['title']})
                league_url = TRANSFER_MARKT_ROOT_URL + league['href']

                league_urls.append(league_url)

            return league_urls
