import json
from time import time

import pandas as pd
from kafka import KafkaProducer

TOPIC = "green-trips"
BOOTSTRAP_SERVERS = "localhost:9092"
PARQUET_FILE = "green_tripdata_2025-10.parquet"

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

def convert_value(col, value):
    if pd.isna(value):
        return None

    if col in ["lpep_pickup_datetime", "lpep_dropoff_datetime"]:
        return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M:%S")

    return value.item() if hasattr(value, "item") else value

def main():
    df = pd.read_parquet(PARQUET_FILE, columns=COLUMNS)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    t0 = time()

    for _, row in df.iterrows():
        message = {col: convert_value(col, row[col]) for col in COLUMNS}
        producer.send(TOPIC, value=message)

    producer.flush()

    t1 = time()
    print(f"Sent {len(df)} records")
    print(f"took {(t1 - t0):.2f} seconds")

if __name__ == "__main__":
    main()