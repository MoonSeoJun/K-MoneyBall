
class Config:
    BOOTSTRAP_SERVERS = ["broker:29092"]

    CLUB_TOPIC = "k_moneyball.sink.clubs"
    CLUB_TOPIC_SOURCE = "k_moneyball.clubs"
    PLAYER_TOPIC = "k_moneyball.sink.players"

    TRANSFER_MARKT_ROOT_URL = "https://www.transfermarkt.com"
    REQUEST_HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}

    CLUBS_INFO_JSON_PATH = './clubs_info.json'
    PLAYERS_INFO_JSON_PATH = './players_info.json'

    KLEAGUE_URLS = [
        "https://www.transfermarkt.com/k-league-1/startseite/wettbewerb/RSK1",
        "https://www.transfermarkt.com/k-league-2/startseite/wettbewerb/RSK2"
    ]