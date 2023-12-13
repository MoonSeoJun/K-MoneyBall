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
                    "_insertedts",
                    "_modifiedts"]

class PostgresqlConnector:
    def __init__(self) -> None:
        pass

    def insert_player_info(self, player_info):
        logging.info(player_info)

        json_query_column = self.__rewrite_json_player_query(player_info)

        logging.info(json_query_column)

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

    def __rewrite_json_player_query(self, json_data) -> dict:
        json_query_column = {}

        for k, v in json_data.items():
            renamed_key = k.lower().replace(' ', '_')
            if renamed_key in PLAYER_BLOCK_KEYS:
                continue
            elif len(renamed_key.split('/')) > 1:
                k_date_of_birth = renamed_key.split('/')[0]
                k_age = renamed_key.split('/')[1]
                v_year = v.split(' ')[2]
                v_month = time.strptime(v.split(' ')[0], '%b').tm_mon
                v_day = v.split(' ')[1][0:-1]
                v_date_of_birth = datetime.datetime.strptime(f"{v_year}-{v_month}-{v_day}", 
                                                            "%Y-%m-%d").date()
                v_age = v.split(' ')[-1][1:3]

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