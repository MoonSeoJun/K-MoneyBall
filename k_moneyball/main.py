from typing import Union
import json

from kafka import KafkaProducer
from fastapi import FastAPI, UploadFile
import pandas as pd

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.post("/stats_file")
def read_stats_file(file: UploadFile):
    stats_data = pd.read_csv(file.file, encoding='CP949')
    stats_data_json = stats_data.to_json(orient='index')
    parsed:dict = json.loads(stats_data_json)

    try:
        producer = KafkaProducer(
            bootstrap_servers=["broker:29092"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8")
        )

        for _, stat in parsed.items():

            producer.send(
                topic="k_moneyball.sink.stats",
                value=stat
            )

        producer.flush()
        producer.close()

        return {"message" : "Stats data sink complete"}
    except:
        return {"message" : "Stats data sink fail"}