from bs4 import BeautifulSoup

import time

TRANSFER_MARKT_ROOT_URL = "https://www.transfermarkt.com"

class PlayerProfileProducer:
    def __init__(self) -> None:
        pass

    def get_player_urls(self, url_content):
        players = []

        soup = BeautifulSoup(url_content, "lxml")

        player_table = soup.find("div", {"class" : "responsive-table"})
        players_info = player_table.find_all("td", {"class" : "hauptlink"})

        for info in players_info:
            if info.a and len(info['class']) == 1:
                player_url = TRANSFER_MARKT_ROOT_URL + info.a['href']
                print(player_url)
                players.append(player_url)

        return players
    
    def get_player_profile(self, url, url_content):
        soup = BeautifulSoup(url_content, "lxml")

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

        player_info["_id"] = url.split('/')[-1]

        time.sleep(1)
        
        return player_info