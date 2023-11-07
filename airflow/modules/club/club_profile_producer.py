
from bs4 import BeautifulSoup

import time

import util.configs as configs

class ClubProfileProducer:
    def __init__(self) -> None:
        pass

    def __parsing_club_profile(self, url_content):
        club_info = []

        soup = BeautifulSoup(url_content, "lxml")

        league_title = soup.find("h1", {"class" : "data-header__headline-wrapper data-header__headline-wrapper--oswald"})
        club_table = soup.find("div", {"class" : "responsive-table"})
        club_tags = club_table.find_all("td", {"class" : "zentriert no-border-rechts"})

        for tag in club_tags:
            club = tag.a['href']
            club_url = configs.TRANSFER_MARKT_ROOT_URL + club

            club_info.append(
            {
                "league" : league_title.string.strip(),
                "club_name" : tag.a['title'],
                "club_url" : club_url
            })

        return club_info
    
    def produce_club_info(self, url_content):
        club_info = self.__parsing_club_profile(url_content)

        return club_info