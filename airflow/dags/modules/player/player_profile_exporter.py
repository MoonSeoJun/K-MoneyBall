from kafka import KafkaProducer

import json
import time


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
            self.producer.send(
                topic=topic,
                value=player_profile
            )

        self.producer.flush()

        time.sleep(0.5)