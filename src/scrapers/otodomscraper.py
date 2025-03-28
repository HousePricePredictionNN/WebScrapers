import requests
from bs4 import BeautifulSoup

from headers import Headers
from scrapers.webpagescraper import WebpageScraper


def scrape_offer_details(link):
    # Initialize dictionary for scraped data
    data = {key: '' for key in [list(Headers)[i].value for i in range(len(Headers))]}

    # Make a call and parse the page
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    page = requests.get(link, headers=headers)
    soup = BeautifulSoup(page.content, 'html.parser')

    try:
        m2 = float(next(filter(lambda x: 'm²' in x.text, soup.find_all('button'))).text.split('m')[0])
        price_m2 = float(
            next(filter(lambda x: 'aria-label' in x.attrs.keys(), soup.find_all('div'))).text.replace(' ', '').split('z')[
                0])
        rooms = float(next(filter(lambda x: 'pok' in x.text, soup.find_all('button'))).text.split('m')[0].split(' ')[0])
        rent = next(filter(lambda x: 'czynsz' in x.text.lower(), soup.find_all('p'))).parent.text.split(':')[-1]
        if 'brak' in rent:
            rent = ''
        else:
            rent = float(rent.replace(' ', '').replace(',', '.').split('z')[0])
        market = next(filter(lambda x: 'rynek' in x.text.lower(), soup.find_all('p'))).parent.text.split(':')[-1]
    except:
        m2 = ''
        price_m2 = ''
        rooms = ''
        rent = ''
        market = ''
    finally:
        data[Headers.M2.value] = m2
        data[Headers.PRICE_PER_M2.value] = price_m2
        data[Headers.ROOMS.value] = rooms
        data[Headers.RENT.value] = rent
        data[Headers.MARKET.value] = market
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
