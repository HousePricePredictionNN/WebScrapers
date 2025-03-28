import random
import time

import requests
from bs4 import BeautifulSoup

import pandas as pd
from tqdm import tqdm

from headers import Headers
from scrapers.olxscraper import OlxScraper, scrape_offer_details as get_olx_offer
from scrapers.otodomscraper import OtodomScraper, scrape_offer_details as get_otodom_offer



def get_offer_details(links):
    # Initialize dictionary for scraped data
    scraped_data = {key: [] for key in [list(Headers)[i].value for i in range(len(Headers))]}

    # Iterate over links of offers
    for link in tqdm(links, 'Scraping offer details'):
        time.sleep(random.randint(1, 2))

        # Use appropriate scraper based on the domain
        if 'otodom' in link:
            data = get_otodom_offer(link)
        else:
            data = get_olx_offer(link)

        for key in scraped_data.keys():
            scraped_data[key].append(data[key])


    return scraped_data


def remove_duplicates(data):
    for link in data[Headers.LINK.value]:
        for _ in range(data[Headers.LINK.value].count(link)-1):
            index = data[Headers.LINK.value].index(link)
            for key in data.keys():
                data[key].pop(index)
    return data


def main():

    # Scraper settings
    olx_scraper = OlxScraper()
    otodom_scraper = OtodomScraper()

    # Data file
    data_file = '../resources/offers_2.csv'

    # Get basic data from the main pages
    data_olx = olx_scraper.scrape_offers(pages=25)
    data_otodom = otodom_scraper.scrape_offers(pages=50)

    # Combine data from both scrapers
    data = {key: [] for key in [list(Headers)[i].value for i in range(len(Headers))]}
    for key in data.keys():
        data[key] = data_olx[key] + data_otodom[key]

    offers_data = get_offer_details(data[Headers.LINK.value])

    # Update basic data with offer details under the same link (they contain complimentary data)
    for key in data.keys():
        if data[key][0] == '':
            data[key] = offers_data[key]

    # Remove duplicate entries
    data = remove_duplicates(data)

    # Save to .csv
    dataframe = pd.DataFrame(data)
    dataframe.to_csv(data_file, index=False, encoding='utf-8')



if __name__ == '__main__':
    main()
