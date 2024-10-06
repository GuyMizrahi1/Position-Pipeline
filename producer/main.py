import os
import pickle
import logging
import datetime
import pandas as pd
from kafka import KafkaProducer
from scraper import scrape_open_positions
from kafka.errors import KafkaError, NoBrokersAvailable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_and_send_parquet(positions: list, location_str: str, department_str: str, kafka_producer: KafkaProducer,
                            kafka_topic: str) -> None:
    """Creates a parquet file with job positions and sends it to Kafka."""
    try:
        current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")
        file_name = f"{location_str}_{department_str}_{current_time}.parquet"

        positions.sort()
        df = pd.DataFrame({"Index": range(1, len(positions) + 1), "Position Title": positions})
        parquet_data = df.to_parquet(index=False)
        future = kafka_producer.send(kafka_topic, key=file_name.encode("utf-8"),
                                     value=pickle.dumps(parquet_data, protocol=5))

        try:
            record_metadata = future.get(timeout=10)  # Wait for message to be sent
            logger.info(f"Message sent to Kafka topic '{kafka_topic}', with key '{file_name}': {record_metadata}")
        except KafkaError as e:
            logger.error(f"Error sending data to Kafka: {e}")
    except Exception as e:
        logger.error(f"Error creating or sending parquet data: {e}")


def main():
    event_hubs_connection_string = os.environ.get("EVENT_HUBS_CONNECTION_STRING")
    event_hub_name = os.environ.get("EVENT_HUB_NAME")
    ssl_certificate_path = os.environ.get("SSL_CERTIFICATE_PATH")
    kafka_topic = os.environ.get("KAFKA_TOPIC")

    required_env_vars = [event_hubs_connection_string, event_hub_name, ssl_certificate_path, kafka_topic]

    for env_var in required_env_vars:
        if not env_var:
            logger.error(f"Error: {env_var} environment variable must be set.")
            exit(1)

    try:
        producer = KafkaProducer(
            bootstrap_servers=f"{event_hub_name}.servicebus.windows.net:9093",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username="$ConnectionString",
            sasl_plain_password=event_hubs_connection_string,
            ssl_cafile=ssl_certificate_path,
            value_serializer=lambda v: pickle.dumps(v)
        )

    except NoBrokersAvailable as e:
        logger.error(f"Error connecting to Event Hubs: {e}")
        exit(1)

    positions, location_str, department_str = scrape_open_positions("https://wsc-sports.com/Careers")
    create_and_send_parquet(positions, location_str, department_str, producer, kafka_topic)


if __name__ == "__main__":
    main()
