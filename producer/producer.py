import csv
import json
from typing import Any

from kafka import KafkaProducer

TOPIC_NAME = "data"

if TOPIC_NAME is None:
    raise ValueError("topic not set")

producer = KafkaProducer(bootstrap_servers=["localhost:9092"])


def format_json(row: dict[str | Any, str | Any]) -> bytes:
    return json.dumps(row).encode()


for i in range(10):
    print("start file " + str(i))
    with open(f"./data/MOCK_DATA ({i}).csv") as f:
        reader = csv.DictReader(f)
        next(reader)  # скип заголовка

        for row in reader:
            msg = format_json(row)
            producer.send(TOPIC_NAME, msg)

    producer.flush()
    print("finished file " + str(i))
