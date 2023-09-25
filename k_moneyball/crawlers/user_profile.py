import requests
from bs4 import BeautifulSoup

import csv
import time
from typing import List

from configs import REQUEST_HEADERS, TRANSFER_MARKT_ROOT_URL

class UserProfile:
    def __init__(self) -> None:
        self.national_num_kr = 87
        self.root_url = f"{TRANSFER_MARKT_ROOT_URL}/wettbewerbe/national/wettbewerbe/87"
        self.league_urls = [
            "https://www.transfermarkt.com/k-league-1/startseite/wettbewerb/RSK1",
            "https://www.transfermarkt.com/k-league-2/startseite/wettbewerb/RSK2"]
        return

    def get_league_urls(self) -> list[dict]:
        # league_urls = []

        # req = requests.get(self.root_url, headers=REQUEST_HEADERS)

        # if req.status_code == requests.codes.ok:
        #     soup = BeautifulSoup(req.text, "lxml")

        #     league_cup_table = soup.find("div", {"class" : "responsive-table"})
        #     league_cups_tags = league_cup_table.find_all("img", {"class" : "continental-league-emblem"})

        #     for tag in league_cups_tags:
        #         league = soup.find("a", {"title" : tag['title']})
        #         league_url = TRANSFER_MARKT_ROOT_URL + league['href']

        #         league_urls.append(league_url)

            return self.league_urls
        
    def get_club_urls(self):
        league_urls = self.get_league_urls()
        club_urls = []

        club_csv = open('club.csv', 'a', newline='')
        wr = csv.writer(club_csv)

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

                    wr.writerow([tag.a['title'], league_title.string.strip()])

                print(url + " Complete =====================")
                time.sleep(3)

        club_csv.close()
        
        print(club_urls)
        return club_urls

    def get_player_profile_urls(self):
        club_urls = self.get_club_urls()

        players = []

        for url in club_urls:
            print(url + " Start =====================")
            req = requests.get(url, headers=REQUEST_HEADERS)

            if req.status_code == requests.codes.ok:
                soup = BeautifulSoup(req.text, "lxml")

                player_table = soup.find("div", {"class" : "responsive-table"})
                players_info = player_table.find_all("td", {"class" : "hauptlink span-va-middle"})

                for info in players_info:
                    players.append(TRANSFER_MARKT_ROOT_URL + info.a['href'])

                print(players)
                print(f"total players : {len(players)}")

                print(url + " Complete =====================")
                time.sleep(3)


        return players

    def get_player_info(self):
        player_urls = self.get_player_profile_urls()
        # player_urls = ["https://www.transfermarkt.com/nana-boateng/profil/spieler/236272"]

        if player_urls is None:
            return

        players = []

        player_csv = open('player.csv', 'a', newline='')
        wr = csv.writer(player_csv)

        failed_url = []

        for url in player_urls:
            print(url + " Start =====================")
            req = None
            try:
                req = requests.get(url, headers=REQUEST_HEADERS)
            except:
                failed_url.append(url)
                print("========================= Failed URL =========================")
                print(failed_url)
                continue

            if req.status_code == requests.codes.ok:
                soup = BeautifulSoup(req.text, "lxml")

                player_table = soup.find("div", {"class" : "large-6 large-pull-6 small-12 columns spielerdatenundfakten"})
                players_info = player_table.find_all("span", {"class" : "info-table__content info-table__content--bold"})

                player_value = soup.find("div", {"class" : "tm-player-market-value-development__current-value"})
                player_club = soup.find("span", {"class" : "data-header__club"})
                player_national = player_table.find("img", {"class" : "flaggenrahmen"}) 

                player = []
                try:
                    if player_value.a:
                        player.append(player_value.a.string.strip())
                    else:
                        player.append(player_value.string.strip())
                except:
                    player.append('0')

                player.append(player_club.find("a")['title'].strip())
                player.append(player_national['title'])

                for i in players_info:
                    if i.string is not None and i.string != "right" and i.string != "left":
                        player.append(i.string.strip())
                    elif i.a:
                        if i.a.string is not None:
                            player.append(i.a.string.strip())

                while len(player) > 10:
                    player.pop()

                print(player)
                wr.writerow(player)
                print(url + " Complete =====================")
                time.sleep(1)
        
        player_csv.close()


user = UserProfile()
user.get_player_info()