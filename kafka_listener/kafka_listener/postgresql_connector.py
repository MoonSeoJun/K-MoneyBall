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

    def insert_mongo_source(self, target_table, data):
        json_query_column = self.__rewrite_json_query(target_table=target_table, 
                                                      json_data=data)

        with psycopg.connect("""dbname=k_moneyball
                                     user=k_moneyball
                                     password=k_moneyball
                                     host=postgres_kmoneyball
                                     port=5432""") as conn:
            with conn.cursor() as cur:
                try:
                    if target_table == 'game_stats':
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
                    cur.execute("INSERT INTO {0} ({1}) VALUES ({2})".format(target_table, columns, values_len), values)
                    conn.commit()
                except TypeError:
                    logging.info('Can not find player or club')

    def update_postgresql(self, target_table, target_id, data):
        json_query_column = self.__rewrite_json_query(target_table=target_table,
                                                      json_data=data)
        
        target_id_column = target_table[0:-1] + "_id"

        with psycopg.connect("""dbname=k_moneyball
                                     user=k_moneyball
                                     password=k_moneyball
                                     host=postgres_kmoneyball
                                     port=5432""") as conn:
            with conn.cursor() as cur:
                columns = ','.join("{0}=%s".format(k) for k, _ in json_query_column.items())
                values = [v for _, v in json_query_column.items()]
                cur.execute("UPDATE {0} SET {1} WHERE {2}={3}".format(target_table, columns, target_id_column, target_id), values)
                conn.commit()

    def __rewrite_json_query(self, target_table, json_data):
        if target_table == 'players':
            return self.__rewrite_json_player_query(json_data)
        elif target_table == 'clubs':
            return self.__rewrite_json_club_query(json_data)
        elif target_table == 'game_stats':
            return self.__rewrite_json_game_stat_query(json_data)

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
