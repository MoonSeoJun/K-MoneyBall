from kafka import KafkaProducer

import k_moneyball.modules.util.configs as configs
from k_moneyball.modules.club.club_profile_producer import ClubProfileProducer

class ClubProfileExporter:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=configs.KAFKA_BOOTSTRAP
        )
        pass

    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        pass
    
    def export_club_info(self, club_info):
        print(club_info)

with ClubProfileExporter() as exporter:
    producer = ClubProfileProducer()
    club_info = producer.produce_club_info()
    exporter.export_club_info(club_info)