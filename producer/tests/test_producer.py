import os
import logging
import unittest
import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from unittest.mock import MagicMock, patch
from producer.main import create_and_send_parquet, main
from selenium.webdriver.remote.webelement import WebElement
from producer.scraper import process_filters, get_filter_path, insert_position_names_into_position_list, \
    organize_position_list, scrape_open_positions

logger = logging.getLogger(__name__)


class TestProducerMain(unittest.TestCase):

    @patch("producer.main.KafkaProducer")
    @patch("producer.main.datetime")
    @patch("producer.main.logger")
    def test_create_and_send_parquet(self, mock_logger, mock_datetime, mock_kafka_producer):
        """Test creating and sending a parquet file to Kafka."""
        positions = ["Data Engineer", "Data Scientist"]
        location_str = "All-Locations"
        department_str = "All-Departments"
        kafka_topic = "test-topic"
        mock_producer = MagicMock()
        mock_kafka_producer.return_value = mock_producer

        fixed_datetime = datetime.datetime(2024, 10, 5, 16, 20, 0, 0)
        mock_datetime.datetime.now.return_value = fixed_datetime

        create_and_send_parquet(positions, location_str, department_str, mock_producer, kafka_topic)

        mock_producer.send.assert_called_once()
        expected_log_message = (
            f"Message sent to Kafka topic '{kafka_topic}', with key "
            f"'{location_str}_{department_str}_{fixed_datetime.strftime('%Y-%m-%d_%H-%M-%S_%f')}.parquet': "
        )
        actual_log_message = mock_logger.info.call_args[0][0]
        self.assertIn(expected_log_message, actual_log_message[:actual_log_message.find("id=")])

    @patch("producer.main.KafkaProducer")
    @patch("producer.main.scrape_open_positions")
    @patch("producer.main.create_and_send_parquet")
    def test_main(self, mock_create_and_send_parquet, mock_scrape_open_positions, mock_kafka_producer):
        load_dotenv()
        kafka_topic = os.environ.get("KAFKA_TOPIC")
        """Test the main function."""
        mock_scrape_open_positions.return_value = (
            ["Data Engineer", "Data Scientist"], "All-Locations", "All-Departments")
        mock_producer = MagicMock()
        mock_kafka_producer.return_value = mock_producer

        main()

        mock_scrape_open_positions.assert_called_once_with("https://wsc-sports.com/Careers")
        mock_create_and_send_parquet.assert_called_once_with(["Data Engineer", "Data Scientist"], "All-Locations",
                                                             "All-Departments", mock_producer, kafka_topic)


class TestProducerScraper(unittest.TestCase):

    def test_process_filters(self):
        """Test processing location and department filters."""
        mock_location_filter = MagicMock(spec=WebElement)
        mock_location_filter.text.strip.return_value = "Tel-Aviv"
        mock_department_filter = MagicMock(spec=WebElement)
        mock_department_filter.text.strip.return_value = "Data Engineer"

        location, department = process_filters(mock_location_filter, mock_department_filter)

        self.assertEqual(location, "Tel-Aviv")
        self.assertEqual(department, "Data-Engineer")

    def test_get_filter_path(self):
        """Test generating the CSS selector path for a filter."""
        root_prefix = "body .global-wrapper .content"
        filter_index = 1
        expected_path = ("body .global-wrapper .content .block-careers-filter .row .col-md-6 .careers-filter "
                         ".field:nth-child(1) .jstyling-select-t")

        self.assertEqual(get_filter_path(root_prefix, filter_index), expected_path)

    def test_insert_position_names_into_position_list(self):
        """Test inserting position names from web elements into a list."""
        positions = []
        mock_position_elements = [MagicMock(spec=WebElement, text="Backend Developer"),
                                  MagicMock(spec=WebElement, text="Data Scientist")]

        insert_position_names_into_position_list(positions, mock_position_elements)

        self.assertEqual(positions, ["Backend Developer", "Data Scientist"])

    def test_organize_position_list(self):
        """Test organizing position names from the parsed HTML."""
        html_content = """
        <div class="block-careers-list">
            <div class="container-fluid">
                <div class="list">
                    <h3>ISRAEL</h3>
                    <ul>
                        <li><a><span class="link-text">Backend Developer</span></a></li>
                        <li><a><span class="link-text">Customer Success Manager</span></a></li>
                    </ul>
                    <h3>LONDON</h3>
                    <ul>
                        <li><a><span class="link-text">Customer Success Manager</span></a></li>
                    </ul>
                </div>
            </div>
        </div>
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        root_prefix = ''

        positions = organize_position_list(soup, root_prefix)

        self.assertEqual(positions, ["Backend Developer", "Customer Success Manager", "Customer Success Manager"])

    @patch("producer.scraper.webdriver.Remote")
    @patch("producer.scraper.WebDriverWait")
    @patch("producer.scraper.BeautifulSoup")
    def test_scrape_open_positions(self, mock_beautifulsoup, mock_webdriverwait, mock_remote):
        """Test scraping job position titles."""
        mock_driver = MagicMock()
        mock_remote.return_value = mock_driver
        mock_soup = MagicMock()
        mock_beautifulsoup.return_value = mock_soup
        mock_soup.select_one.return_value = None
        mock_soup.select.return_value = []

        # Mock the find_element method to return the expected text values
        mock_location_filter = MagicMock()
        mock_location_filter.text.strip.return_value = "All-Locations"
        mock_department_filter = MagicMock()
        mock_department_filter.text.strip.return_value = "All-Departments"
        mock_driver.find_element.side_effect = [mock_location_filter, mock_department_filter]

        positions, location_str, department_str = scrape_open_positions("https://wsc-sports.com/Careers")

        self.assertEqual(positions, [])
        self.assertEqual(location_str, "All-Locations")
        self.assertEqual(department_str, "All-Departments")


if __name__ == "__main__":
    unittest.main()
