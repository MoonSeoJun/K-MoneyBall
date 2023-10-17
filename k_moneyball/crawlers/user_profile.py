import requests
from bs4 import BeautifulSoup

import csv
import time

from configs import REQUEST_HEADERS, TRANSFER_MARKT_ROOT_URL, DEFAULT_DATA_DICT, PLAYER_PROFILE_CSV_COLUMN
from club_profile import ClubProfile

class UserProfile:
    def get_player_profile_urls(self):
        club_profile = ClubProfile()
        club_urls = club_profile.get_club_urls(is_writing_csv=True)

        players, failed_url = self.get_player_urls(club_urls)

        while len(failed_url) != 0:
            print("========================= Start Failed URL =========================")
            ply, failed_url = self.get_player_urls(failed_url)
            players += ply
            print(f"Add Failed url / total players : {len(players)}")

        return players

    def get_player_urls(self, club_urls):
        players = []
        failed_url = []

        for url in club_urls:
            print(url + " Start =====================")
            try:
                req = requests.get(url, headers=REQUEST_HEADERS)
            except:
                print("========================= Failed URL =========================")
                failed_url.append(url)
                print(failed_url)
                continue

            if req.status_code == requests.codes.ok:
                soup = BeautifulSoup(req.text, "lxml")

                player_table = soup.find("div", {"class" : "responsive-table"})
                players_info = player_table.find_all("td", {"class" : "hauptlink span-va-middle"})

                for info in players_info:
                    players.append(TRANSFER_MARKT_ROOT_URL + info.a['href'])

                print(players)
                print(f"total players : {len(players)}")

                print(url + " Complete =====================")
                time.sleep(2)

        return players, failed_url

    def get_player_info(self):
        player_urls = self.get_player_profile_urls()

        if player_urls is None:
            return
        
        failed_url = self.write_player_csv(player_urls=player_urls)

        while len(failed_url) != 0:
            print("Failed URL Start =====================")
            failed_url = self.write_player_csv(player_urls=failed_url)
    
    def write_player_csv(self, player_urls):
        player_csv = open('player5.csv', 'a', newline='')
        wr = csv.writer(player_csv)
        wr.writerow(PLAYER_PROFILE_CSV_COLUMN)

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
                player_shirt_info = soup.find("h1", {"class" : "data-header__headline-wrapper"})
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

                player_shirt_info_cleared = player_shirt_info.get_text().replace(" ", "").split("\n")
                player.append(player_shirt_info_cleared[-1])
                player.append(player_shirt_info_cleared[-2])
                player.append(player_club.find("a")['title'].strip())
                player.append(player_national['title'])

                player_info_title = player_table.find_all("span", {"class" : "info-table__content info-table__content--regular"})
                player_info_value = player_table.find_all("span", {"class" : "info-table__content info-table__content--bold"})

                unnecessary_keys = []

                for i in player_info_title:
                    title = i.string.strip().replace(":", "")
                    if title.find("Current") != -1 or title.find("Social") != -1 or title.find("agent") != -1:
                        unnecessary_keys.append(player_info_title.index(i))
                    player_info_title[player_info_title.index(i)] = title

                if len(unnecessary_keys) != 0:
                    unnecessary_keys.reverse()

                    for key in unnecessary_keys:                    
                        player_info_title.pop(key)

                player_info = {player_info_title[i] : player_info_value[i] for i in range(len(player_info_title))}

                for k, v in DEFAULT_DATA_DICT.items():
                    if player_info.get(k):
                        v = player_info.get(k)
                        if v.string is not None:
                            player.append(v.string.strip())
                        elif v.text:
                            player.append(v.text.strip())
                        elif v.a:
                            if v.a.string is not None:
                                player.append(v.a.string.strip())
                    else:
                        player.append(None)

                print(player)
                wr.writerow(player)
                print(url + " Complete =====================")
                time.sleep(1)
        
        player_csv.close()

        return failed_url

user = UserProfile()
user.get_player_info()
