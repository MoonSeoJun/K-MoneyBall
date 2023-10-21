import requests
from bs4 import BeautifulSoup

import time
import json

from configs import REQUEST_HEADERS, TRANSFER_MARKT_ROOT_URL, CLUBS_INFO_JSON_PATH

class ClubProfile:
    def __init__(self):
        self.national_num_kr = 87
        self.root_url = f"{TRANSFER_MARKT_ROOT_URL}/wettbewerbe/national/wettbewerbe/87"
        self.league_urls = [
            "https://www.transfermarkt.com/k-league-1/startseite/wettbewerb/RSK1",
            "https://www.transfermarkt.com/k-league-2/startseite/wettbewerb/RSK2"]

    def get_league_urls(self):
        return self.league_urls
        
    def extract_club_info(self):
        league_urls = self.get_league_urls()
        club_info = []

        for url in league_urls:
            print(url + " Start =====================")
            req = requests.get(url, headers=REQUEST_HEADERS)

            if req.status_code == requests.codes.ok:
                soup = BeautifulSoup(req.text, "lxml")

                league_title = soup.find("h1", {"class" : "data-header__headline-wrapper data-header__headline-wrapper--oswald"})
                club_table = soup.find("div", {"class" : "responsive-table"})
                club_tags = club_table.find_all("td", {"class" : "zentriert no-border-rechts"})

                for tag in club_tags:
                    club = tag.a['href']
                    club_url = TRANSFER_MARKT_ROOT_URL + club

                    club_info.append(
                    {
                        "league" : league_title.string.strip(),
                        "club_name" : tag.a['title'],
                        "club_url" : club_url
                    })

                print(url + " Complete =====================")
                time.sleep(2)

        with open(CLUBS_INFO_JSON_PATH, 'w') as outfile:
            json.dump({
                "clubs" : club_info
            }, outfile, indent='\t')

club = ClubProfile()
club.extract_club_info()