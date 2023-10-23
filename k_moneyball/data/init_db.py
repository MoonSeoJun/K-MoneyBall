from pymongo.mongo_client import MongoClient

import json

from config import PLAYERS_INFO_JSON_PATH, CLUBS_INFO_JSON_PATH

url = "mongodb://admin:admin@mongo:27017"
client = MongoClient(url)

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

football_player_info_db = client.get_database('k_moneyball')
football_player_info_db.create_collection('clubs')
football_player_info_db.create_collection('players')


change_event_info_db = client.get_database('event_sink')
change_event_info_db.create_collection('club')
change_event_info_db.create_collection('player')


db = client.get_database('k_moneyball')

clubs_colc = db.get_collection('clubs')
players_colc = db.get_collection('players')

with open(CLUBS_INFO_JSON_PATH, 'r') as club_file:
    club_data = json.load(club_file)

with open(PLAYERS_INFO_JSON_PATH, 'r') as player_file:
    player_data = json.load(player_file)

print(clubs_colc.insert_many(club_data['clubs']))
print(players_colc.insert_many(player_data['players']))