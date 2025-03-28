import random
import time
from abc import ABC, abstractmethod

from tqdm import tqdm

from headers import Headers


class WebpageScraper(ABC):

    @abstractmethod
    def __init__(self):
        self.domain = None
        self.endpoint = None

        self.t1 = None
        self.t2 = None


    @abstractmethod
    def scrape_page(self, page: int):
        pass


    @abstractmethod
    def scrape_offers(self, pages: int):
        scraped_data = {key: [] for key in [list(Headers)[i].value for i in range(len(Headers))]}
        for p in tqdm(range(1, pages + 1), 'Scraping offers from pages'):
            time.sleep(random.randint(self.t1, self.t2))
            data = self.scrape_page(p)

            # Append data to dictionary
            for key in scraped_data.keys():
                values = data[key]
                if len(values) == 0:
                    values = ['']*len(data[Headers.LINK.value])
                scraped_data[key].extend(values)

        return scraped_data
