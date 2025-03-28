from enum import Enum


class Headers(Enum):
    TITLE = 'tytuł ogłoszenia'
    LINK = 'link'
    LOCATION = 'lokalizacja'
    M2 = 'powierzchnia'
    TOTAL_PRICE = 'cena'
    PRICE_PER_M2 = 'cena za metr'
    RENT = 'czynsz'
    ROOMS = 'liczba pokoi'
    BUILDING_TYPE = 'rodzaj budynku'
    MARKET = 'rynek'
