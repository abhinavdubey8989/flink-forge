import os
from dataclasses import dataclass
from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Config:
    kafka_bootstrap_servers: str
    kafka_topic: str
    event_batch_id: str
    event_count: int
    total_users: int
    sleep_ms: int
    event_types: list[str]


def load_config() -> Config:
    return Config(
        kafka_bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        kafka_topic=os.environ["KAFKA_TOPIC"],
        event_batch_id=os.environ["EVENT_BATCH_ID"],
        event_count=int(os.environ["EVENT_COUNT"]),
        total_users=int(os.environ["TOTAL_USERS"]),
        sleep_ms=int(os.environ["SLEEP_MS"]),
        event_types=[
            event.strip()
            for event in os.environ["EVENT_TYPES"].split(",")
            if event.strip()
        ],
    )