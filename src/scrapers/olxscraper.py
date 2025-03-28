import time
import random

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from headers import Headers
from src.scrapers.webpagescraper import WebpageScraper


def scrape_offer_details(link: str):
    data = {key: '' for key in [list(Headers)[i].value for i in range(len(Headers))]}

    page = requests.get(link)
    soup = BeautifulSoup(page.content, 'html.parser')

    paragraphs = soup.find_all('p')

    try:
        m2 = float(next(filter(lambda x: 'powierzchnia' in x.text.lower(), paragraphs)).text.split(':')[-1].strip().split(' ')[0].replace(',', '.'))
        price_m2 = float(next(filter(lambda x: 'cena za' in x.text.lower(), paragraphs)).text.split(':')[-1].strip().split(' ')[0])
        rooms = float(next(filter(lambda x: 'liczba pokoi' in x.text.lower(), paragraphs)).text.split(':')[-1].strip().split(' ')[0])
        building_type = next(filter(lambda x: 'rodzaj zabudowy' in x.text.lower(), paragraphs)).text.split(':')[-1].strip()
        market = next(filter(lambda x: 'rynek' in x.text.lower(), paragraphs)).text.split(':')[-1].strip()
    except:
        m2 = ''
        price_m2 = ''
        rooms = ''
        building_type = ''
        market = ''
    finally:
        data[Headers.M2.value] = m2
        data[Headers.PRICE_PER_M2.value] = price_m2
        data[Headers.ROOMS.value] = rooms
        data[Headers.BUILDING_TYPE.value] = building_type
        data[Headers.MARKET.value] = market
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
