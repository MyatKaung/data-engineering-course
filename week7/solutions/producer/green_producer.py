import json
import math
import time

import pandas as pd
from kafka import KafkaProducer


TOPIC_NAME = "green-trips"
BOOTSTRAP_SERVERS = ["localhost:9092"]
DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
COLUMNS = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
    "total_amount",
]


def json_serializer(message: dict) -> bytes:
    return json.dumps(message).encode("utf-8")


def normalize_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, float):
        if math.isnan(value):
            return None
        return float(value)
    return value


def row_to_message(row: pd.Series) -> dict:
    return {column: normalize_value(row[column]) for column in COLUMNS}


def main():
    df = pd.read_parquet(DATA_URL, columns=COLUMNS)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=json_serializer,
    )

    t0 = time.time()

    for _, row in df.iterrows():
        producer.send(TOPIC_NAME, value=row_to_message(row))

    producer.flush()

    t1 = time.time()
    print(f"sent {len(df)} rows to {TOPIC_NAME}")
    print(f"took {(t1 - t0):.2f} seconds")


if __name__ == "__main__":
    main()
