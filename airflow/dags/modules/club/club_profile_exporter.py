from kafka import KafkaProducer

import json
import time


class ClubProfileExporter:
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode("utf-8")
        )

    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        self.producer.close()
    
    def export_club_info(self, topic, club_info):
        for club in club_info:
            self.producer.send(
                topic=topic,
                value=club
            )
            time.sleep(0.05)

        self.producer.flush()