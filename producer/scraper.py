import logging
from bs4 import BeautifulSoup
from typing import Tuple, List
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_filters(location_filter: WebElement, department_filter: WebElement) -> Tuple[str, str]:
    if location_filter and location_filter.text.strip() != "All Locations":
        logger.info(f"Location filter is set to '{location_filter.text.strip()}'.")

    if department_filter and department_filter.text.strip() != "All Departments":
        logger.info(f"Department filter is set to '{department_filter.text.strip()}'.")

    location = location_filter.text.strip().replace(" ", "-") if location_filter else "All-Locations"
    department = department_filter.text.strip().replace(" ", "-") if department_filter else "All-Departments"
    return location, department


def get_filter_path(root_prefix: str, filter_index: int) -> str:
    """Generates the CSS selector path for a filter."""
    return (f'{root_prefix} .block-careers-filter .row .col-md-6 .careers-filter .field:nth-child({filter_index}) '
            f'.jstyling-select-t')


def insert_position_names_into_position_list(positions: List[str], position_elements: List[WebElement]) -> None:
    for element in position_elements:
        position_name = element.text.strip()
        positions.append(position_name)


def organize_position_list(soup: BeautifulSoup, root_prefix: str) -> List[str]:
    positions = []
    no_positions_message = soup.select_one(f'{root_prefix} .block-careers-list .text.text-center')
    if no_positions_message:
        logger.info(f'No open positions found.')
        return positions

    countries_section = soup.select(f'{root_prefix} .block-careers-list .container-fluid .list h3')

    if not countries_section:  # Single country
        position_elements = soup.select(f'{root_prefix} .block-careers-list .container-fluid .list > ul li a .link-text')
        insert_position_names_into_position_list(positions, position_elements)
    else:  # Multiple countries
        for section in countries_section:
            country_name = section.text.strip()
            logger.info(f'The following positions are from {country_name}')
            position_elements = section.find_next_sibling('ul').select('li a .link-text')
            insert_position_names_into_position_list(positions, position_elements)

    return positions


def scrape_open_positions(url: str) -> Tuple[List[str], str, str]:
    """Scrapes job position titles, handling different country and 'no positions' scenarios."""
    driver = webdriver.Chrome()
    driver.get(url)

    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.block-careers-list')))

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    root_prefix = 'body .global-wrapper .content'

    location_filter = driver.find_element(By.CSS_SELECTOR, get_filter_path(root_prefix, 1))
    department_filter = driver.find_element(By.CSS_SELECTOR, get_filter_path(root_prefix, 2))

    location_str, department_str = process_filters(location_filter, department_filter)

    positions = organize_position_list(soup, root_prefix)

    driver.quit()
    return positions, location_str, department_str
