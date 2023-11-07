
from bs4 import BeautifulSoup

import time

import util.configs as configs
from club.club_profile_vaildator import UrlVaildator

class ClubProfileProducer:
    def __init__(self) -> None:
        self.url_vaildator = UrlVaildator()
    
    def __parsing_club_profile(self, url):
        print(url + " Start =====================")
        url_content = self.url_vaildator.verify_url(target_url=url)

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

        print(url + " Complete =====================")

        return club_info
    
    def produce_club_info(self):
        club_info = []

        for url in configs.KLEAGUE_URLS:
            club_info.extend(self.__parsing_club_profile(url))
            time.sleep(2)

        return club_info