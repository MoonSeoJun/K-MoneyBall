from pymongo.mongo_client import MongoClient

import json

from config import PLAYERS_INFO_JSON_PATH, CLUBS_INFO_JSON_PATH

def read_collection_all_data(collection):
    cursor = collection.find()

    while True:
        try:
            print(cursor.next())
        except StopIteration as e:
            print(e)
            break

url = "mongodb://admin:admin@mongo:27017"
client = MongoClient(url)

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

db = client.get_database('football_player_info')

with open(CLUBS_INFO_JSON_PATH, 'r') as club_file:
    club_data = json.load(club_file)

with open(PLAYERS_INFO_JSON_PATH, 'r') as player_file:
    player_data = json.load(player_file)

print(db.drop_collection('clubs'))
print(db.drop_collection('players'))

clubs_colc = db.create_collection('clubs')
players_colc = db.create_collection('players')

print(clubs_colc.insert_many(club_data['clubs']))
print(players_colc.insert_many(player_data['players']))

read_collection_all_data(clubs_colc)
read_collection_all_data(players_colc)