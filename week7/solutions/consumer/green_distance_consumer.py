import json

from kafka import KafkaConsumer


TOPIC_NAME = "green-trips"
BOOTSTRAP_SERVERS = ["localhost:9092"]


def deserialize_message(data: bytes) -> dict:
    return json.loads(data.decode("utf-8"))


def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id="green-distance-counter",
        consumer_timeout_ms=5000,
        value_deserializer=deserialize_message,
    )

    total_rows = 0
    trips_over_5 = 0

    for message in consumer:
        total_rows += 1
        trip = message.value
        trip_distance = trip.get("trip_distance")

        if trip_distance is not None and float(trip_distance) > 5:
            trips_over_5 += 1

    consumer.close()

    print(f"consumed {total_rows} rows from {TOPIC_NAME}")
    print(f"trip_distance > 5: {trips_over_5}")


if __name__ == "__main__":
    main()
