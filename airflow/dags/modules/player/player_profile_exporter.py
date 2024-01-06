from kafka import KafkaProducer

import json
import time
import datetime
import re
import logging


class PlayerProfileExporter:
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode("utf-8")
        )

    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        self.producer.close()
    
    def export_player_profile(self, topic, player_profiles):
        for player_profile in player_profiles:
            preprocessed_player_profile = self.__rewrite_json_player_query(player_profile)

            self.producer.send(
                topic=topic,
                value=preprocessed_player_profile
            )

        self.producer.flush()

        time.sleep(0.5)

    def __rewrite_json_player_query(self, json_data) -> dict:
        json_query_column = {}

        for k, v in json_data.items():
            renamed_key = k.lower().replace(' ', '_')
            if v == "":
                json_query_column[renamed_key] = None
            elif len(renamed_key.split('/')) > 1:
                try:
                    k_date_of_birth = renamed_key.split('/')[0]
                    k_age = renamed_key.split('/')[1]
                    v_year = v.split(' ')[2]
                    v_month = time.strptime(v.split(' ')[0], '%b').tm_mon
                    v_day = v.split(' ')[1][0:-1]
                    v_date_of_birth = datetime.datetime.strptime(f"{v_year}-{v_month}-{v_day}", 
                                                                "%Y-%m-%d").date()
                    v_age = v.split(' ')[3][1:3]

                    json_query_column[k_date_of_birth] = str(v_date_of_birth)
                    json_query_column[k_age] = v_age
                except:
                    logging.info("Can not parsing date of birth/age")
            elif renamed_key == "joined" or renamed_key == "contract_expires" or renamed_key == "date_of_last_contract_extension":
                if v == "-":
                    json_query_column[renamed_key] = None
                else:
                    v_year = v.split(' ')[2]
                    v_month = time.strptime(v.split(' ')[0], '%b').tm_mon
                    v_day = v.split(' ')[1][0:-1]
                    v_date_of_birth = datetime.datetime.strptime(f"{v_year}-{v_month}-{v_day}", 
                                                                "%Y-%m-%d").date()
                    json_query_column[renamed_key] = str(v_date_of_birth)
            elif renamed_key == "height" or renamed_key == "shirt_num":
                only_num = int(re.sub(r"[^0-9]", "", v))
                json_query_column[renamed_key] = only_num
            elif renamed_key == "market_value":
                only_num = int(re.sub(r"[^0-9]", "", v))
                if v[-1] == "k":
                    only_num /= 10
                json_query_column[renamed_key] = only_num
            else:
                json_query_column[renamed_key] = v

        return json_query_column