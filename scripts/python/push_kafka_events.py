#!/usr/bin/env python3

import random
import time
import json
from kafka import KafkaProducer
from config import load_config


config = load_config()


def create_producer():
    return KafkaProducer(
        bootstrap_servers=config.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        acks="all",
    )


def get_random_user_id():
    return f"user_{random.randint(1, config.total_users)}"


def get_event_payload(idx, user_id=None, event_type=None):

    # Set random user-id
    if user_id is None:
        user_id = get_random_user_id()

    # Set random event-type
    if event_type is None:
        event_type = random.choice(config.event_types)

    event_payload = {
        "eventId": f"{config.event_batch_id}_{idx}",

        # time.time() gives unix time in seconds,
        # converting it into millisecond
        "ts": int(time.time() * 1000),

        "userId": user_id,
        "eventType": event_type}
    return event_payload


def publish_events():

    success = 0
    failed = 0
    total_events_to_send = config.event_count
    producer = create_producer()

    for i in range(total_events_to_send):
        try:
            # Prepare payload to send to kafka topic
            event_payload = get_event_payload(i)

            # This is synchronous way of sending messages to kafka
            # producer.send
            # - Returns immediately after queueing the message in the producer's internal buffer
            # - A background I/O thread sends the message to Kafka
            # metadata = future.get
            # - This makes the process sync
            # - future.get() blocks until that specific message has been acknowledged (or fails)
            future = producer.send(config.kafka_topic, value=event_payload)
            metadata = future.get(timeout=2) # timeout in seconds

            print(
                f"[{i}/{total_events_to_send}] Sent to : "
                f"{metadata.topic}-{metadata.partition}@{metadata.offset}"
            )
            success += 1

            # Sleep for the configured duration
            # time.sleep expected in seconds, but in env file, we have it in milliseconds
            time.sleep(config.sleep_ms / 1000.0)

        except Exception as e:
            print(f"[{i}/{total_events_to_send}] Failed: {e}")
            failed += 1

    # This blocks until all buffered records have been sent to Kafka (or have failed)
    # Without flush(), your program might exit while messages are still sitting in the producer's internal buffer.
    producer.flush()

    # shuts down the Kafka producer and releases its resources.
    producer.close()

    print(f"\nCompleted")
    print(f"Successful : {success}")
    print(f"Failed     : {failed}")


def main():
    publish_events()


if __name__ == "__main__":
    main()
