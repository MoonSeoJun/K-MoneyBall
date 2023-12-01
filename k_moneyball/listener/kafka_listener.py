from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

import time
import logging
import json

def create_consumer():
    bootstrap_servers = ['broker:29092']
    group_id = 'k_moneyball'
    topic = ['k_moneyball.clubs', 'k_moneyball.players', 'k_moneyball.stats']

    while True:
        try:
            consumer = KafkaConsumer(
                group_id=group_id,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',  # Read messages from the beginning when no offset is stored
            )

            consumer.subscribe(topics=topic)

            print("Kafka connection Complete!!")

            return consumer
        except NoBrokersAvailable:
            print(f"connect kafka error by NoBrokersAvailable")
            time.sleep(5)

def consume_messages(consumer):
    try:
        for msg in consumer:
            decoded_msg = msg.value.decode('utf-8')
            msg_topic = msg.topic
            print("============================================")
            print(f"msg_topic : {msg_topic}")
            json_decoded_msg = json.loads(decoded_msg)
            print(f"Received message: {json_decoded_msg}")
            print(f"Received message: {type(json_decoded_msg)}")
            print("============================================")

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("Start Kafka connection")
    kafka_consumer = create_consumer()
    consume_messages(kafka_consumer)
