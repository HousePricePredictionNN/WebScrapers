import requests
from bs4 import BeautifulSoup

from headers import Headers
from scrapers.webpagescraper import WebpageScraper


def scrape_offer_details(link):
    # Initialize dictionary for scraped data
    data = {key: '' for key in [list(Headers)[i].value for i in range(len(Headers))]}
    data[Headers.LINK.value] = link

    # Make a call and parse the page
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    
    try:
        page = requests.get(link, headers=headers)
        soup = BeautifulSoup(page.content, 'html.parser')
        
        # Find the main details container div (with property info)
        details_container = soup.find('div', class_=lambda cls: cls and ('css-8mnxk5' in cls or 'ellui0j0' in cls))
        
        if details_container:
            # Extract area (m²) from button
            for button in details_container.find_all('button'):
                if 'm²' in button.text:
                    area_text = button.text.strip()
                    area_value = area_text.split('m²')[0].strip()
                    try:
                        data[Headers.M2.value] = float(area_value)
                        break
                    except (ValueError, TypeError):
                        pass
            
            # Extract number of rooms from button
            for button in details_container.find_all('button'):
                if any(room_text in button.text.lower() for room_text in ['pokoje', 'pokój', 'pokoj']):
                    rooms_text = button.text.strip()
                    rooms_value = rooms_text.split('pok')[0].strip()
                    try:
                        data[Headers.ROOMS.value] = float(rooms_value)
                        break
                    except (ValueError, TypeError):
                        pass
            
            # Extract key-value pairs from div elements
            detail_pairs = details_container.find_all('div', class_=lambda cls: cls and 'css-1xw0jqp' in cls)
            for div in detail_pairs:
                p_elements = div.find_all('p')
                if len(p_elements) >= 2:
                    key = p_elements[0].text.strip().replace(':', '').lower()
                    value = p_elements[1].text.strip()
                    
                    # Map keys to our Headers enum values
                    if 'czynsz' in key:
                        if 'brak' not in value.lower():
                            try:
                                # Remove non-numeric chars except decimal separator
                                rent_value = ''.join(c for c in value if c.isdigit() or c == '.' or c == ',')
                                rent_value = rent_value.replace(',', '.')
                                data[Headers.RENT.value] = float(rent_value)
                            except (ValueError, TypeError):
                                pass
                    elif 'rynek' in key:
                        data[Headers.MARKET.value] = value
                    elif 'rodzaj zabudowy' in key:
                        data[Headers.BUILDING_TYPE.value] = value
                    elif 'ogrzewanie' in key:
                        data[Headers.HEATING.value] = value
                    elif 'piętro' in key:
                        data[Headers.FLOOR.value] = value
                    elif 'stan wykończenia' in key:
                        data[Headers.FINISH_CONDITION.value] = value
                    elif 'forma własności' in key:
                        data[Headers.OWNERSHIP_FORM.value] = value
                    elif 'dostępne od' in key:
                        data[Headers.AVAILABLE_FROM.value] = value
                    elif 'typ ogłoszeniodawcy' in key:
                        data[Headers.ADVERTISER_TYPE.value] = value
                    elif 'rok budowy' in key:
                        data[Headers.BUILDING_YEAR.value] = value
                    elif 'winda' in key:
                        data[Headers.ELEVATOR.value] = value
                    elif 'materiał budynku' in key:
                        data[Headers.BUILDING_MATERIAL.value] = value
                    elif 'okna' in key:
                        data[Headers.WINDOWS.value] = value
                    elif 'certyfikat energetyczny' in key:
                        data[Headers.ENERGY_CERTIFICATE.value] = value
            
            # Extract additional information (features with checkmarks)
            additional_info = []
            equipment = []
            security = []
            media = []
            
            # Find sections with additional information
            info_sections = details_container.find_all('div', class_='css-1xw0jqp')
            for section in info_sections:
                section_title = section.find('p')
                if section_title and section_title.text:
                    title_text = section_title.text.lower().strip()
                    if 'informacje dodatkowe' in title_text:
                        features = section.find_all('span', class_=lambda cls: cls and 'css-axw7ok' in cls)
                        additional_info = [feature.text.strip() for feature in features]
                    elif 'wyposażenie' in title_text:
                        features = section.find_all('span', class_=lambda cls: cls and 'css-axw7ok' in cls)
                        equipment = [feature.text.strip() for feature in features]
                    elif 'zabezpieczenia' in title_text:
                        features = section.find_all('span', class_=lambda cls: cls and 'css-axw7ok' in cls)
                        security = [feature.text.strip() for feature in features]
                    elif 'media' in title_text:
                        features = section.find_all('span', class_=lambda cls: cls and 'css-axw7ok' in cls)
                        media = [feature.text.strip() for feature in features]
            
            data[Headers.ADDITIONAL_INFO.value] = ', '.join(additional_info) if additional_info else ''
            data[Headers.EQUIPMENT.value] = ', '.join(equipment) if equipment else ''
            data[Headers.SECURITY.value] = ', '.join(security) if security else ''
            data[Headers.MEDIA.value] = ', '.join(media) if media else ''
        
        # Find the price per m² (often in a div with price information)
        price_per_m2_elements = soup.find_all(string=lambda text: text and 'zł/m²' in text)
        for element in price_per_m2_elements:
            try:
                price_text = element.strip().replace(' ', '').split('zł/m²')[0]
                data[Headers.PRICE_PER_M2.value] = float(price_text)
                break
            except (ValueError, TypeError):
                pass
        
        # Find the total price
        price_elements = soup.find_all(string=lambda text: text and 'zł' in text and 'zł/m²' not in text)
        for element in price_elements:
            try:
                price_text = ''.join(c for c in element if c.isdigit() or c == '.')
                price = float(price_text)
                if price > 1000:
                    data[Headers.TOTAL_PRICE.value] = price
                    break
            except (ValueError, TypeError):
                pass
                
    except Exception as e:
        print(f"Error scraping {link}: {e}")
    
    return data


class OtodomScraper(WebpageScraper):
    def __init__(self, time_sleep: tuple[int, int] = (1, 2)):
        super().__init__()
        self.domain = 'https://www.otodom.pl'
        self.endpoint = f'{self.domain}/pl/wyniki/sprzedaz/mieszkanie/lodzkie/lodz/lodz/lodz?viewType=listing'

        self.t1 = time_sleep[0]
        self.t2 = time_sleep[1]


    def scrape_page(self, page: int):
        # Get link to the page
        link = f'{self.endpoint}&page={page}'

        # Make a call and parse the page
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        page = requests.get(link, headers=headers)
        soup = BeautifulSoup(page.content, 'html.parser')

        articles = soup.find_all('article')
        data = {key: [] for key in [list(Headers)[i].value for i in range(len(Headers))]}
        for article in articles:
            offer_link = self.domain + article.find('a')['href']
            data[Headers.LINK.value].append(offer_link)

            offer_title = article.find('p', attrs={'data-cy': 'listing-item-title'}).text
            data[Headers.TITLE.value].append(offer_title)

            offer_price = soup.find_all('article')[0].find('span', attrs={'direction': 'horizontal'}).text
            if offer_price == 'Zapytaj o cenę':
                offer_price = ''
            else:
                offer_price = float(''.join(offer_price.split()[:-1]))
            data[Headers.TOTAL_PRICE.value].append(offer_price)

            offer_location = soup.find_all('article')[0].find_all('p')[-1].text
            data[Headers.LOCATION.value].append(offer_location)

        return data



    def scrape_offers(self, pages: int):
        return super().scrape_offers(pages)
