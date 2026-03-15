import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "green-trips",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="green-trips-test",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    consumer_timeout_ms=5000
)

count = 0
gt_5 = 0

for msg in consumer:
    count += 1
    trip = msg.value
    distance = trip.get("trip_distance")

    if distance is not None and float(distance) > 5.0:
        gt_5 += 1

print("messages read:", count)
print("trip_distance > 5:", gt_5)