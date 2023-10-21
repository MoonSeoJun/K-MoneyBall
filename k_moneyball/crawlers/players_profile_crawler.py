import requests
from bs4 import BeautifulSoup

import time
import json
import os

from configs import REQUEST_HEADERS, TRANSFER_MARKT_ROOT_URL, CLUBS_INFO_JSON_PATH, PLAYERS_INFO_JSON_PATH

class UserProfile:
    def get_player_profile_urls(self):
        with open(CLUBS_INFO_JSON_PATH, 'r') as f:
            clubs_data = json.load(f)

        failed_url = self.__get_player_urls(clubs_data['clubs'])

        while len(failed_url) != 0:
            print("========================= Start Failed URL =========================")
            failed_url = self.__get_player_urls(failed_url)

    def __get_player_urls(self, club_info):
        failed_url = []

        for club in club_info:
            players = []
            url = club['club_url']
            print(url + " Start =====================")
            try:
                req = requests.get(url, headers=REQUEST_HEADERS)
            except:
                print("========================= Failed URL =========================")
                failed_url.append(club)
                print(failed_url)
                continue

            if req.status_code == requests.codes.ok:
                soup = BeautifulSoup(req.text, "lxml")

                player_table = soup.find("div", {"class" : "responsive-table"})
                players_info = player_table.find_all("td", {"class" : "hauptlink"})

                for info in players_info:
                    if info.a and len(info['class']) == 1:
                        player = {
                            "club" : club['club_name'],
                            "url" : TRANSFER_MARKT_ROOT_URL + info.a['href']
                        }
                        print(player)
                        players.append(player)

                if os.path.exists(PLAYERS_INFO_JSON_PATH):
                    with open(PLAYERS_INFO_JSON_PATH, 'r') as f:
                        json_data = json.load(f)
                    
                    with open(PLAYERS_INFO_JSON_PATH, 'w') as f:
                        json_data['players'] += players
                        json.dump(json_data, f, indent='\t')
                        print(f"total players : {len(json_data['players'])}")
                else:
                    with open(PLAYERS_INFO_JSON_PATH, 'w') as f:
                        json.dump({
                            "players" : players
                        }, f, indent='\t')
                    print(f"total players : {len(players)}")

                print(url + " Complete =====================")

                time.sleep(2)

        return failed_url

    def get_player_info(self):        
        failed_url = self.__write_player_detail_info()

        while len(failed_url) != 0:
            print("Failed URL Start =====================")
            failed_url = self.__write_player_detail_info(player_urls=failed_url)
    
    def __write_player_detail_info(self, failed_url=None):
        if failed_url is None:
            with open(PLAYERS_INFO_JSON_PATH, 'r') as f:
                player_data = json.load(f)
        else:
            player_data = failed_url

        failed_url = []

        for player in player_data['players']:
            url = player['url']
            print(url + " Start =====================")
            req = None
            try:
                req = requests.get(url, headers=REQUEST_HEADERS)
            except:
                failed_url.append(player)
                print("========================= Failed URL =========================")
                print(failed_url)
                continue

            if req.status_code == requests.codes.ok:
                soup = BeautifulSoup(req.text, "lxml")

                player_table = soup.find("div", {"class" : "large-6 large-pull-6 small-12 columns spielerdatenundfakten"})

                player_info_title = player_table.find_all("span", {"class" : "info-table__content info-table__content--regular"})
                player_info_value = player_table.find_all("span", {"class" : "info-table__content--bold"})

                player_info_table = []

                for i in player_info_title:
                    title = i.string.strip().replace(":", "")
                    player_info_title[player_info_title.index(i)] = title

                for v in player_info_value:
                    if v.string:
                        player_info_table.append(v.string.strip())
                    elif v.text:
                        player_info_table.append(v.text.strip())

                player_info = {player_info_title[i] : player_info_table[i] for i in range(len(player_info_title))}

                player_value = soup.find("a", {"class" : "data-header__market-value-wrapper"})
                try:
                    player_info['market_value'] = player_value.text.split(' ')[0][1:]
                except:
                    player_info['market_value'] = '0'

                player_shirt_info = soup.find("h1", {"class" : "data-header__headline-wrapper"})
                player_shirt_info_cleared = player_shirt_info.get_text().replace(" ", "").split("\n")
                player_info['shirt_num'] = player_shirt_info_cleared[-2]
                player_info['shirt_name'] = player_shirt_info_cleared[-1]

                player_national = player_table.find("img", {"class" : "flaggenrahmen"})
                player_info['national'] = player_national['title']

                player_info['url'] = url

                player_data['players'][player_data['players'].index(player)] = player_info

                with open(PLAYERS_INFO_JSON_PATH, 'w') as f:
                    json.dump(player_data, f, indent='\t')

                print(url + " Complete =====================")
                time.sleep(1)
        
        return failed_url

user = UserProfile()
user.get_player_profile_urls()
user.get_player_info()