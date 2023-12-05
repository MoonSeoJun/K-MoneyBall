from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

import time
import logging
import json
import datetime

def create_producer():
    bootstrap_servers = ['broker:29092']

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

            print("Kafka consumer connection Complete!!")

            return consumer
        except NoBrokersAvailable:
            print(f"connect kafka error by NoBrokersAvailable")
            time.sleep(5)

def consume_messages(consumer:KafkaConsumer, producer:KafkaProducer):
    try:
        for msg in consumer:
            decoded_msg = msg.value.decode('utf-8')
            msg_topic = msg.topic
            print("============================================")
            print(f"msg_topic : {msg_topic}")
            json_decoded_msg = json.loads(decoded_msg)
            print(f"Received message: {json_decoded_msg}")
            print("============================================")

            msg_payload = json.loads(json_decoded_msg['payload'])

            if msg_payload['operationType'] == "update":
                event_document = msg_payload["ns"]["coll"]
                event_document_oid = msg_payload["documentKey"]["_id"]["$oid"]
                event_description = msg_payload["updateDescription"]
                event_timestamp = datetime.datetime.now()

                event_sink = {
                    'targetDocument' : event_document,
                    'eventTarget' : event_document_oid,
                    'updateDescription' : event_description,
                    'eventTimestamp' : str(event_timestamp)
                }

                producer.send(
                    topic='k_moneyball.sink.event',
                    value=event_sink
                )
                producer.flush()

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("Start Kafka connection")
    kafka_consumer = create_consumer()
    kafka_producer = create_producer()
    consume_messages(kafka_consumer, kafka_producer)
