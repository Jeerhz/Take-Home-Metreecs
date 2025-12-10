from pydantic_settings import BaseSettings
import os

# path to the folder containing data csv files
DATA_DIR = "./data/assignment/"
# path to the folder to save result files
RESULT_DIR = "./data/results/"
# path to the folder to save cache files. This serves to keep mapping of UUIDS and values
CACHE_DIR = "./data/cache/"
# path to the file containing mapping of original sku to product UUID
PRODUCT_UUID_MAP_PATH = f"{CACHE_DIR}product_uuid_map.json"
# path to the file containing mapping of original location city abbreviation to location UUID
LOCATION_UUID_MAP_PATH = f"{CACHE_DIR}location_uuid_map.json"
# filenames in the data folder
LOCATION_FILENAME = "locations.csv"
PRODUCTS_FILENAME = "products.csv"
PRODUCTS_ATTRIBUTES_FILENAME = "product_attributes.csv"
SALES_FILENAME = "sales.csv"
STOCKS_FILENAME = "stocks.csv"
WEATHER_FILENAME = "weather.csv"


class Settings(BaseSettings):
    LOCATIONS_CSV_PATH: str
    PRODUCTS_CSV_PATH: str
    PRODUCTS_ATTRIBUTES_FILENAME: str
    WEATHER_FILENAME: str
    SALES_CSV_PATH: str
    STOCKS_CSV_PATH: str
    PRODUCT_UUID_MAP_PATH: str
    LOCATION_UUID_MAP_PATH: str
    RESULT_DIR: str
    CACHE_DIR: str

    @property
    def location_filename(self) -> str:
        return os.path.basename(self.LOCATIONS_CSV_PATH)

    @property
    def products_filename(self) -> str:
        return os.path.basename(self.PRODUCTS_CSV_PATH)

    @property
    def sales_filename(self) -> str:
        return os.path.basename(self.SALES_CSV_PATH)

    @property
    def stocks_filename(self) -> str:
        return os.path.basename(self.STOCKS_CSV_PATH)


settings = Settings(
    LOCATIONS_CSV_PATH=f"{DATA_DIR}/{LOCATION_FILENAME}",
    PRODUCTS_CSV_PATH=f"{DATA_DIR}/{PRODUCTS_FILENAME}",
    PRODUCTS_ATTRIBUTES_FILENAME=PRODUCTS_ATTRIBUTES_FILENAME,
    WEATHER_FILENAME=WEATHER_FILENAME,
    SALES_CSV_PATH=f"{DATA_DIR}/{SALES_FILENAME}",
    STOCKS_CSV_PATH=f"{DATA_DIR}/{STOCKS_FILENAME}",
    PRODUCT_UUID_MAP_PATH=PRODUCT_UUID_MAP_PATH,
    LOCATION_UUID_MAP_PATH=LOCATION_UUID_MAP_PATH,
    RESULT_DIR=RESULT_DIR,
    CACHE_DIR=CACHE_DIR,
)
