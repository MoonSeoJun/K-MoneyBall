import psycopg

import logging
import datetime

PLAYER_BLOCK_KEYS = ["contract_there_expires",
                    "social-media",
                    "outfitter",
                    "full_name",
                    "2nd_club",
                    "_modifiedts"]

CLUB_BLOCK_KEYS = ["_modifiedTS"]

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
                if target_table == 'game_stats':
                    try:
                        cur.execute("SELECT player_id FROM players WHERE (current_club = '{0}' and shirt_name = '{1}')"
                                        .format(json_query_column['club'], json_query_column['name']))
                        plyaer_info = cur.fetchone()
                        if plyaer_info is None:
                            cur.execute("SELECT player_id, shirt_name FROM players WHERE (current_club = '{0}' and shirt_num = {1})"
                                        .format(json_query_column['club'], json_query_column['shirt_number']))
                            plyaer_info = cur.fetchone()
                            json_query_column['name'] = plyaer_info[1]

                        player_id = plyaer_info[0]
                        json_query_column['player_id'] = player_id
                        

                        cur.execute("SELECT club_id FROM clubs WHERE club_name = '{0}'"
                                        .format(json_query_column['club']))
                        club_id = cur.fetchone()[0]
                        json_query_column['club_id'] = club_id

                        cur.execute("SELECT club_id FROM clubs WHERE club_name = '{0}'"
                                            .format(json_query_column['match']))
                        match_club_id = cur.fetchone()[0]
                        json_query_column['match_club_id'] = match_club_id
                    except TypeError:
                        print("Can not find player or club")
                        return

                    columns = ','.join(json_query_column.keys())
                    values = [v for _, v in json_query_column.items()]
                    values_len = ','.join('%s' for _ in range(len(values)))
                    cur.execute("INSERT INTO {0} ({1}) VALUES ({2})".format(target_table, columns, values_len), values)
                    conn.commit()
                elif target_table == "players":
                    columns = ','.join(json_query_column.keys())
                    values = [v for _, v in json_query_column.items()]
                    values_len = ','.join('%s' for _ in range(len(values)))
                    cur.execute("INSERT INTO players_history ({0}) VALUES ({1})".format(columns, values_len), values)

                    update_columns = ','.join("{0}=%s".format(k) for k, _ in json_query_column.items() if k != "_id")
                    columns = ','.join("{0}".format(k) for k, _ in json_query_column.items() if k != "_id")
                    values = [v for k, v in json_query_column.items() if k != "_id"]
                    values_len = ','.join('%s' for _ in range(len(values)))
                    values.extend(values)
                    cur.execute("INSERT INTO players ({0}) VALUES ({1}) ON CONFLICT (player_id) DO UPDATE SET {2}"
                                .format(columns, values_len, update_columns), values)
                    conn.commit()
                elif target_table == "clubs":
                    columns = ','.join(json_query_column.keys())
                    values = [v for _, v in json_query_column.items()]
                    values_len = ','.join('%s' for _ in range(len(values)))
                    cur.execute("INSERT INTO clubs_history ({0}) VALUES ({1})".format(columns, values_len), values)

                    update_columns = ','.join("{0}=%s".format(k) for k, _ in json_query_column.items() if k != "_id")
                    columns = ','.join("{0}".format(k) for k, _ in json_query_column.items() if k != "_id")
                    values = [v for k, v in json_query_column.items() if k != "_id"]
                    values_len = ','.join('%s' for _ in range(len(values)))
                    values.extend(values)
                    cur.execute("INSERT INTO clubs ({0}) VALUES ({1}) ON CONFLICT (club_id) DO UPDATE SET {2}"
                                .format(columns, values_len, update_columns), values)
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
            if type(v) is dict:
                if '$date' in v.keys():
                    date = int(v.get('$date'))
                    v = datetime.datetime.fromtimestamp(date/1000)
                    k = "timestamp"
                elif '$numberLong' in v.keys():
                    v = float(v.get('$numberLong'))
                elif '$oid':
                    v = v.get('$oid')
                json_query_column[k] = v
            else:
                json_query_column[renamed_key] = v

        return json_query_column
    
    def __rewrite_json_club_query(self, json_data:dict) -> dict:
        json_query_column = {}

        for k, v in json_data.items():
            if k in CLUB_BLOCK_KEYS:
                continue
            if type(v) is dict:
                if '$date' in v.keys():
                    date = int(v.get('$date'))
                    v = datetime.datetime.fromtimestamp(date/1000)
                    k = "timestamp"
                elif '$oid':
                    v = v.get('$oid')
                json_query_column[k] = v
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
                    v = float(v.get('$numberLong'))
                elif '$oid':
                    v = v.get('$oid')
            json_query_column[k] = v

        return json_query_column
