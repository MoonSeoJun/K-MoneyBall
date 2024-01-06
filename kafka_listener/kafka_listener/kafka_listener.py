from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

import time
import logging
import json

from postgresql_connector import PostgresqlConnector

class KafkaListener:
    def __init__(self) -> None:
        bootstrap_servers = ['broker:29092']
        topic = ['k_moneyball.clubs', 'k_moneyball.players', 'k_moneyball.game_stats']

        self.consumer = self.__create_consumer(bootstrap_servers, topic)
        self.postgresql_connector = PostgresqlConnector()

    def consume_messages(self):
        try:
            for msg in self.consumer:
                decoded_msg = msg.value.decode('utf-8')
                msg_topic = msg.topic
                print("============================================")
                print(f"msg_topic : {msg_topic}")
                json_decoded_msg = json.loads(decoded_msg)
                print(f"Received message: {json_decoded_msg}")

                msg_payload = json.loads(json_decoded_msg['payload'])
                event_operation_type = msg_payload['operationType']
                event_document = msg_payload["ns"]["coll"]
                print("============================================")

                if event_operation_type == "insert":
                    insert_data = msg_payload['fullDocument']
                    self.postgresql_connector.insert_mongo_source(target_table=event_document,
                                                                  data=insert_data)

        except KeyboardInterrupt:
            pass

        finally:
            self.consumer.close()

    def __create_consumer(self, bootstrap_servers, topic):
        group_id = 'k_moneyball'

        while True:
            try:
                consumer = KafkaConsumer(
                    group_id=group_id,
                    bootstrap_servers=bootstrap_servers,
                    auto_offset_reset='earliest',  # Read messages from the beginning when no offset is stored
                )

                consumer.subscribe(topics=topic)

                print("Kafka consumer connection Complete!!")

                return consumer
            except NoBrokersAvailable:
                print(f"connect kafka error by NoBrokersAvailable")
                time.sleep(5)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("Start Kafka connection")

    kafka_listener = KafkaListener()
    kafka_listener.consume_messages()
