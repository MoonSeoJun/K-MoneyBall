from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

import time
import logging
import json

class KafkaListener:
    def __init__(self) -> None:
        bootstrap_servers = ['broker:29092']
        topic = ['k_moneyball.clubs', 'k_moneyball.players', 'k_moneyball.game_stats']

        self.producer = self.__create_producer(bootstrap_servers)
        self.consumer = self.__create_consumer(bootstrap_servers, topic)

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
                event_document_id = msg_payload["documentKey"]["_id"]
                print("============================================")

                event_sink = {
                    'event_operation_type' : event_operation_type,
                    'targetDocument' : event_document,
                    'eventTarget' : event_document_id
                }

                if event_operation_type == "update":
                    event_description = msg_payload["updateDescription"]
                    updated_field = event_description['updatedFields']
                    if len(updated_field) == 1: continue
                    else: event_sink['updateDescription'] = event_description
                else:
                    event_sink['fullDocument'] = msg_payload['fullDocument']

                self.producer.send(
                    topic='k_moneyball.sink.event',
                    value=event_sink
                )
                self.producer.flush()

        except KeyboardInterrupt:
            pass

        finally:
            self.consumer.close()

    def __create_producer(self, bootstrap_servers):
        while True:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda x: json.dumps(x).encode("utf-8")
                )

                print("Kafka producer connection Complete!!")

                return producer
            except NoBrokersAvailable:
                print(f"connect kafka error by NoBrokersAvailable")
                time.sleep(5)

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
