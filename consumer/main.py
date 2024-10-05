import os
import io
import pickle
import logging
import threading
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from azure.storage.blob import BlobServiceClient
from kafka.consumer.fetcher import ConsumerRecord
from azure.core.exceptions import ResourceNotFoundError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def upload_parquet_to_azure_blob_storage(df: pd.DataFrame, blob_service_client: BlobServiceClient, container_name: str,
                                         file_name: str) -> None:
    """Uploads a DataFrame to Azure Blob Storage as a parquet file."""
    try:
        parquet_buffer = df.to_parquet(index=False)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        blob_client.upload_blob(parquet_buffer, overwrite=True)
        logger.info(f"Uploaded data to Azure Blob: {container_name}/{file_name}")
    except ResourceNotFoundError as e:
        logger.error(f"Error: Blob container '{container_name}' not found. {e}")
    except Exception as e:
        logger.error(f"Error uploading data to Azure Blob Storage: {e}")


def process_and_upload_data(message: ConsumerRecord, blob_service_client: BlobServiceClient,
                            container_name: str) -> None:
    """Processes a Kafka message (pickled parquet data) and uploads to Azure Blob Storage."""
    try:
        file_name = message.key.decode("utf-8") if message.key else "positions.parquet"

        parquet_bytes = pickle.loads(message.value)

        with io.BytesIO(parquet_bytes) as f:
            df = pd.read_parquet(f, engine='pyarrow')

        logger.info(f"Top 5 rows of the DataFrame:\n{df.head().to_string(index=False)}")

        upload_parquet_to_azure_blob_storage(df, blob_service_client, container_name, file_name)

        logger.info(f"Processed and uploaded '{file_name}' from Kafka message: {message.offset}")
    except Exception as e:
        logger.error(f"Error processing or uploading data: {e}")


def main():
    load_dotenv()

    event_hubs_connection_string = os.environ.get("EVENT_HUBS_CONNECTION_STRING")
    event_hub_name = os.environ.get("EVENT_HUB_NAME")
    ssl_certificate_path = os.environ.get("SSL_CERTIFICATE_PATH")
    kafka_topic = os.environ.get("KAFKA_TOPIC")
    azure_storage_connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    blob_container_name = os.environ.get("BLOB_CONTAINER_NAME")

    required_env_vars = [event_hubs_connection_string, event_hub_name, ssl_certificate_path, kafka_topic,
                         azure_storage_connection_string, blob_container_name]

    for env_var in required_env_vars:
        if not env_var:
            logger.error(f"Error: {env_var} environment variable must be set.")
            exit(1)

    # Kafka Consumer configuration
    try:
        consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=f"{event_hub_name}.servicebus.windows.net:9093",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username="$ConnectionString",
            sasl_plain_password=event_hubs_connection_string,
            ssl_cafile=ssl_certificate_path,
            value_deserializer=lambda x: pickle.loads(x),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            consumer_timeout_ms=30000,
            group_id='open-positions-consumer-group'
        )
    except KafkaError as e:
        logger.error(f"Error connecting to Event Hubs: {e}")
        exit(1)

    try:
        blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
    except Exception as e:
        logger.error(f"Error connecting to Azure Blob Storage: {e}")
        exit(1)

    timer = threading.Timer(30, lambda: logger.info("Consumer loop timed out started."))
    timer.start()

    for message in consumer:
        timer.cancel()
        process_and_upload_data(message, blob_service_client, blob_container_name)

        timer = threading.Timer(30, lambda: logger.info("Consumer loop timed out restarted."))
        timer.start()

    logger.info("Consumer exiting gracefully.")


if __name__ == "__main__":
    main()
