import io
import pickle
import logging
import unittest
import pandas as pd
from unittest.mock import MagicMock, patch
from kafka.consumer.fetcher import ConsumerRecord
from consumer.main import upload_parquet_to_azure_blob_storage, process_and_upload_data

logger = logging.getLogger(__name__)


class TestConsumerFunctions(unittest.TestCase):

    @patch("consumer.main.BlobServiceClient")
    @patch("consumer.main.logger")
    def test_upload_parquet_to_azure_blob_storage(self, mock_logger, mock_blob_service_client):
        """Test uploading a DataFrame to Azure Blob Storage."""
        df = pd.DataFrame({"Index": [1, 2], "Position title": ["Data Engineer", "Data Scientist"]})
        mock_blob_client = MagicMock()
        mock_blob_service_client.get_blob_client.return_value = mock_blob_client

        upload_parquet_to_azure_blob_storage(df, mock_blob_service_client, "test-container", "test_data.parquet")

        mock_blob_client.upload_blob.assert_called_once()
        mock_logger.info.assert_any_call("Uploaded data to Azure Blob: test-container/test_data.parquet")

    @patch("consumer.main.BlobServiceClient")
    @patch("consumer.main.logger")
    def test_process_and_upload_data(self, mock_logger, mock_blob_service_client):
        """Test processing and uploading data from a Kafka message."""
        df = pd.DataFrame({"Index": [1, 2], "Position title": ["Data Engineer", "Data Scientist"]})
        parquet_bytes = io.BytesIO()
        df.to_parquet(parquet_bytes, index=False)
        parquet_bytes.seek(0)
        message = ConsumerRecord(
            topic="test-topic",
            partition=0,
            offset=0,
            timestamp=0,
            timestamp_type=0,
            key=b"test_key",
            value=pickle.dumps(parquet_bytes.read()),
            checksum=None,
            serialized_key_size=8,
            serialized_value_size=len(parquet_bytes.getvalue()),
            serialized_header_size=0,
            headers=[]
        )
        mock_blob_client = MagicMock()
        mock_blob_service_client.get_blob_client.return_value = mock_blob_client

        process_and_upload_data(message, mock_blob_service_client, "test-container")

        mock_blob_client.upload_blob.assert_called_once()
        mock_logger.info.assert_any_call(
            "Top 5 rows of the DataFrame:\n" + df.head().to_string(index=False))
        mock_logger.info.assert_any_call("Processed and uploaded 'test_key' from Kafka message: 0")

    @patch("consumer.main.BlobServiceClient")
    @patch("consumer.main.logger")
    def test_no_new_messages(self, mock_logger, mock_blob_service_client):
        """Test handling of no new messages in the Kafka topic."""
        message = None

        try:
            process_and_upload_data(message, mock_blob_service_client, "test-container")
        except Exception as e:
            mock_logger.info.assert_called_with(f"Error processing or uploading data: {e}")


if __name__ == "__main__":
    unittest.main()
