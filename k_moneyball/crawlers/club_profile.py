import requests
from bs4 import BeautifulSoup

import time
import csv

from configs import REQUEST_HEADERS, TRANSFER_MARKT_ROOT_URL, CLUB_PROFILE_CSV_COLUMN

class ClubProfile:
    def __init__(self) -> None:
        self.national_num_kr = 87
        self.root_url = f"{TRANSFER_MARKT_ROOT_URL}/wettbewerbe/national/wettbewerbe/87"
        self.league_urls = [
            "https://www.transfermarkt.com/k-league-1/startseite/wettbewerb/RSK1",
            "https://www.transfermarkt.com/k-league-2/startseite/wettbewerb/RSK2"]
        return

    def get_league_urls(self) -> list[dict]:
        return self.league_urls
        
    def get_club_urls(self, is_writing_csv):
        print(f"=================== is_writing_csv : {is_writing_csv} ===================")
        league_urls = self.get_league_urls()
        club_urls = []

        if is_writing_csv:
            club_csv = open('club5.csv', 'a', newline='')
            wr = csv.writer(club_csv)
            wr.writerow(CLUB_PROFILE_CSV_COLUMN)

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

                    club_urls.append(club_url)

                    print(league_title.string.strip())
                    print(tag.a['title'])

                    if is_writing_csv:
                        wr.writerow([tag.a['title'], league_title.string.strip()])

                print(url + " Complete =====================")
                time.sleep(2)

        if is_writing_csv:
            club_csv.close()
        
        print(club_urls)
        return club_urls
