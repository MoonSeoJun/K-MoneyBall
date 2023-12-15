import psycopg

import logging
import datetime
import time
import re

PLAYER_BLOCK_KEYS = ["on_loan_from", 
                    "contract_option", 
                    "contract_there_expires",
                    "date_of_last_contract_extension",
                    "social-media",
                    "outfitter",
                    "full_name",
                    "2nd_club",
                    "_insertedts",
                    "_modifiedts"]

CLUB_BLOCK_KEYS = ["_insertedTS",
                   "_modifiedTS"]

GAME_STAT_BLOCK_KEYS = ["ID"]

class PostgresqlConnector:
    def __init__(self) -> None:
        pass

    def insert_player_info(self, player_info):
        json_query_column = self.__rewrite_json_player_query(player_info)

        with psycopg.connect("""dbname=k_moneyball
                                     user=k_moneyball
                                     password=k_moneyball
                                     host=postgres_kmoneyball
                                     port=5432""") as conn:
            with conn.cursor() as cur:
                columns = ','.join(json_query_column.keys())
                values = [v for _, v in json_query_column.items()]
                values_len = ','.join('%s' for _ in range(len(values)))
                cur.execute("INSERT INTO players ({0}) VALUES ({1})".format(columns, values_len), values)
                conn.commit()

    def update_player_info(self, player_id, player_info):
        json_query_column = self.__rewrite_json_player_query(player_info)

        with psycopg.connect("""dbname=k_moneyball
                                     user=k_moneyball
                                     password=k_moneyball
                                     host=postgres_kmoneyball
                                     port=5432""") as conn:
            with conn.cursor() as cur:
                columns = ','.join(f"{k}=%s" for k, _ in json_query_column.items())
                values = [v for _, v in json_query_column.items()]
                cur.execute("UPDATE players SET {0} WHERE player_id={1}".format(columns,player_id), values)
                conn.commit()

    def insert_club_info(self, club_info):
        json_query_column = self.__rewrite_json_club_query(club_info)

        with psycopg.connect("""dbname=k_moneyball
                                     user=k_moneyball
                                     password=k_moneyball
                                     host=postgres_kmoneyball
                                     port=5432""") as conn:
            with conn.cursor() as cur:
                columns = ','.join(json_query_column.keys())
                values = [v for _, v in json_query_column.items()]
                values_len = ','.join('%s' for _ in range(len(values)))
                cur.execute("INSERT INTO clubs ({0}) VALUES ({1})".format(columns, values_len), values)
                conn.commit()

    def update_club_info(self, club_id, club_info):
        json_query_column = self.__rewrite_json_club_query(club_info)

        with psycopg.connect("""dbname=k_moneyball
                                     user=k_moneyball
                                     password=k_moneyball
                                     host=postgres_kmoneyball
                                     port=5432""") as conn:
            with conn.cursor() as cur:
                columns = ','.join(f"{k}=%s" for k, _ in json_query_column.items())
                values = [v for _, v in json_query_column.items()]
                cur.execute("UPDATE clubs SET {0} WHERE club_id={1}".format(columns, club_id), values)
                conn.commit()

    def insert_game_stat_info(self, game_stat):
        json_query_column = self.__rewrite_json_game_stat_query(game_stat)

        with psycopg.connect("""dbname=k_moneyball
                                     user=k_moneyball
                                     password=k_moneyball
                                     host=postgres_kmoneyball
                                     port=5432""") as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("SELECT player_id FROM players WHERE (current_club = '{0}' and shirt_num = {1})"
                                .format(json_query_column['club'], json_query_column['shirt_number']))
                    player_id = cur.fetchone()[0]
                    json_query_column['player_id'] = player_id

                    cur.execute("SELECT club_id FROM clubs WHERE club_name = '{0}'"
                                .format(json_query_column['club']))
                    club_id = cur.fetchone()[0]
                    json_query_column['club_id'] = club_id

                    cur.execute("SELECT club_id FROM clubs WHERE club_name = '{0}'"
                                .format(json_query_column['match']))
                    match_club_id = cur.fetchone()[0]
                    json_query_column['match_club_id'] = match_club_id

                    columns = ','.join(json_query_column.keys())
                    values = [v for _, v in json_query_column.items()]
                    values_len = ','.join('%s' for _ in range(len(values)))
                    cur.execute("INSERT INTO game_stats ({0}) VALUES ({1})".format(columns, values_len), values)
                    conn.commit()
                except:
                    logging.info("Can not find player")

    def __rewrite_json_player_query(self, json_data) -> dict:
        json_query_column = {}

        for k, v in json_data.items():
            renamed_key = k.lower().replace(' ', '_')
            if renamed_key in PLAYER_BLOCK_KEYS:
                continue
            if v == "":
                json_query_column[renamed_key] = None
            elif len(renamed_key.split('/')) > 1:
                k_date_of_birth = renamed_key.split('/')[0]
                k_age = renamed_key.split('/')[1]
                v_year = v.split(' ')[2]
                v_month = time.strptime(v.split(' ')[0], '%b').tm_mon
                v_day = v.split(' ')[1][0:-1]
                v_date_of_birth = datetime.datetime.strptime(f"{v_year}-{v_month}-{v_day}", 
                                                            "%Y-%m-%d").date()
                v_age = v.split(' ')[3][1:3]

                json_query_column[k_date_of_birth] = v_date_of_birth
                json_query_column[k_age] = v_age
            elif renamed_key == "joined" or renamed_key == "contract_expires":
                if v == "-":
                    json_query_column[renamed_key] = None
                else:
                    v_year = v.split(' ')[2]
                    v_month = time.strptime(v.split(' ')[0], '%b').tm_mon
                    v_day = v.split(' ')[1][0:-1]
                    v_date_of_birth = datetime.datetime.strptime(f"{v_year}-{v_month}-{v_day}", 
                                                                "%Y-%m-%d").date()
                    json_query_column[renamed_key] = v_date_of_birth
            elif renamed_key == "height" or renamed_key == "shirt_num":
                only_num = re.sub(r"[^0-9]", "", v)
                json_query_column[renamed_key] = only_num
            elif k == "_id":
                json_query_column["player_id"] = v
            else:
                json_query_column[renamed_key] = v

        return json_query_column
    
    def __rewrite_json_club_query(self, json_data:dict) -> dict:
        json_query_column = {}

        for k, v in json_data.items():
            if k in CLUB_BLOCK_KEYS:
                continue
            elif k == "_id":
                json_query_column["club_id"] = v
            else:
                json_query_column[k] = v

        return json_query_column
    
    def __rewrite_json_game_stat_query(self, game_stat:dict) -> dict:
        json_query_column = {}

        for k, v in game_stat.items():
            if k in GAME_STAT_BLOCK_KEYS:
                continue
            if type(v) is dict:
                if '$numberLong' in v.keys():
                    v = int(v.get('$numberLong'))
                elif '$oid':
                    v = v.get('$oid')
            json_query_column[k] = v

        return json_query_column
