import logging
import re

import requests
from bs4 import BeautifulSoup

from headers import Headers
from .webpagescraper import WebpageScraper

logging.basicConfig(level=logging.INFO)

def scrape_offer_details(link: str):
    """Scrape details from a specific OLX offer page."""
    if not link.startswith(('http://', 'https://')):
        logging.error(f"Invalid URL: {link}")
        return {}

    # Initialize dictionary for scraped data
    data = {key: '' for key in [list(Headers)[i].value for i in range(len(Headers))]}
    data[Headers.LINK.value] = link
    
    try:
        # Request page with a generous timeout
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        page = requests.get(link, headers=headers, timeout=10)
        soup = BeautifulSoup(page.content, 'html.parser')
        
        # Extract title
        title_element = soup.find('h4', class_='css-10ofhqw')
        if not title_element:
            title_element = soup.find('h1')  # Fallback to h1 if the specific h4 isn't found
        if title_element:
            data[Headers.TITLE.value] = title_element.text.strip()
            
        # Extract total price
        price_element = soup.find('h3', class_=lambda c: c and 'css-' in c)
        if price_element:
            price_text = price_element.text.strip()
            price_match = re.search(r'(\d[\d\s]*)', price_text)
            if price_match:
                price_value = price_match.group(1).replace(' ', '')
                try:
                    data[Headers.TOTAL_PRICE.value] = float(price_value)
                except ValueError:
                    pass
        
        # Find parameters container
        params_container = soup.find('div', {'data-testid': 'ad-parameters-container'})
        if not params_container:
            params_container = soup.find('div', class_=lambda c: c and 'css-41yf00' in c)
            
        if params_container:
            # Extract all parameter rows
            param_rows = params_container.find_all('div', class_=lambda c: c and 'css-ae1s7g' in c)
            
            for row in param_rows:
                # Get the parameter text
                param_text = row.get_text().strip()
                
                # Extract different parameters based on text content
                if 'Cena za m²:' in param_text:
                    match = re.search(r'([\d\s,.]+)\s*zł/m²', param_text)
                    if match:
                        try:
                            value = match.group(1).replace(' ', '').replace(',', '.')
                            data[Headers.PRICE_PER_M2.value] = float(value)
                        except ValueError:
                            pass
                            
                elif 'Powierzchnia:' in param_text:
                    match = re.search(r'([\d\s,.]+)\s*m²', param_text)
                    if match:
                        try:
                            value = match.group(1).replace(' ', '').replace(',', '.')
                            data[Headers.M2.value] = float(value)
                        except ValueError:
                            pass
                            
                elif 'Liczba pokoi:' in param_text:
                    match = re.search(r'(\d+)\s*pok', param_text)
                    if match:
                        try:
                            data[Headers.ROOMS.value] = float(match.group(1))
                        except ValueError:
                            pass
                            
                elif 'Poziom:' in param_text:
                    match = re.search(r'Poziom:\s*(\d+)', param_text)
                    if match:
                        data[Headers.FLOOR.value] = match.group(1)
                        
                elif 'Umeblowane:' in param_text:
                    match = re.search(r'Umeblowane:\s*(\w+)', param_text)
                    if match:
                        data[Headers.EQUIPMENT.value] = match.group(1)
                        
                elif 'Rynek:' in param_text:
                    match = re.search(r'Rynek:\s*(\w+)', param_text)
                    if match:
                        data[Headers.MARKET.value] = match.group(1)
                        
                elif 'Rodzaj zabudowy:' in param_text:
                    match = re.search(r'Rodzaj zabudowy:\s*([^\n]+)', param_text)
                    if match:
                        data[Headers.BUILDING_TYPE.value] = match.group(1).strip()
        
        # Extract location from breadcrumbs or other elements
        location_elements = soup.find_all('a', href=lambda h: h and '/nieruchomosci/mieszkania/sprzedaz/' in h)
        if location_elements and len(location_elements) > 0:
            # Last breadcrumb is usually the location
            location = location_elements[-1].text.strip()
            if location and not location.lower() == 'sprzedaż':
                data[Headers.LOCATION.value] = location
        
        # Extract additional information from description
        description = soup.find('div', {'data-cy': 'ad_description'})
        if description:
            desc_text = description.text.lower()
            
            # Look for heating type
            heating_patterns = ['ogrzewanie', 'centralne', 'miejskie', 'gazowe', 'elektryczne']
            for pattern in heating_patterns:
                if pattern in desc_text:
                    idx = desc_text.find(pattern)
                    snippet = desc_text[max(0, idx-20):idx+30]
                    if 'ogrzewanie' in snippet:
                        # Try to extract the heating type
                        match = re.search(r'ogrzewanie[\s:]*([\w\s]+)', snippet)
                        if match:
                            data[Headers.HEATING.value] = match.group(1).strip()
                            break
            
            # Extract building year if mentioned
            year_match = re.search(r'rok budowy[:\s]*(\d{4})', desc_text)
            if year_match:
                data[Headers.BUILDING_YEAR.value] = year_match.group(1)
                
            # Check for elevator mentions
            if 'wind' in desc_text:
                if re.search(r'brak\s+wind', desc_text):
                    data[Headers.ELEVATOR.value] = 'nie'
                else:
                    data[Headers.ELEVATOR.value] = 'tak'
                    
    except Exception as e:
        logging.error(f"Error scraping {link}: {str(e)}")
    
    return data


class OlxScraper(WebpageScraper):
    def __init__(self, time_sleep: tuple[int, int] = (1, 2)):
        super().__init__()
        self.domain = 'https://www.olx.pl'
        self.endpoint = f'{self.domain}/nieruchomosci/mieszkania/sprzedaz/lodz'
        self.t1 = time_sleep[0]
        self.t2 = time_sleep[1]

    def scrape_page(self, page: int):
        # Get link to the page
        link = f'{self.endpoint}/?page={page}'

        # Make a call and parse the page
        page = requests.get(link)
        soup = BeautifulSoup(page.content, 'html.parser')

        # Get all divs with offers
        offers_divs = list(filter(lambda x: 'l-card' in x.attrs.values(), soup.find_all('div')))

        # Get details from each offer
        data = {key: [] for key in [list(Headers)[i].value for i in range(len(Headers))]}
        for div in offers_divs:
            # Get offer link
            offer_link = div.find_all('a')[-1].get('href')
            offer_link = self.domain + offer_link if '/d/oferta' in offer_link else offer_link
            data[Headers.LINK.value].append(offer_link)

            # Get offer title
            offer_title = div.find_all('a')[-1].text
            data[Headers.TITLE.value].append(offer_title)

            # Get offer price
            offer_price = float(div.find_all('p')[0].text.replace(' ', '').replace('zł', '').replace('donegocjacji', ''))
            data[Headers.TOTAL_PRICE.value].append(offer_price)

            # Get offer location
            offer_location = div.find_all('p')[-1].text.split(' - ')[0]
            data[Headers.LOCATION.value].append(offer_location)

        return data


    def scrape_offers(self, pages):
        return super().scrape_offers(pages)