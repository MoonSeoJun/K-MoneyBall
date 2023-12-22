
from bs4 import BeautifulSoup

import time

import modules.util.configs as configs

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
                "club_id" : club.split('/')[-3],
                "league" : league_title.string.strip(),
                "club_name" : tag.a['title'],
                "url" : club_url
            })

        time.sleep(1)

        return club_info
    
    def __parsing_current_club_title(self, url_content):
        soup = BeautifulSoup(url_content, "lxml")

        club_title = soup.find("h1", {"class" : "data-header__headline-wrapper data-header__headline-wrapper--oswald"}).string.strip()

        time.sleep(1)

        return club_title
    
    def produce_current_club_title(self, url_content):
        club_title = self.__parsing_current_club_title(url_content)

        return club_title
    
    def produce_club_info(self, url_content):
        club_info = self.__parsing_club_profile(url_content)

        return club_info