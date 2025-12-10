from uuid import UUID
from pydantic import BaseModel
import datetime
import pandas as pd


class Location(BaseModel):
    """Schema for the locations table."""

    id: UUID
    name: str
    latitude: float
    longitude: float


class Product(BaseModel):
    """Schema for the products table."""

    id: UUID
    category: str
    is_perishable: bool


class ProductAttribute(BaseModel):
    """Schema for the product_attributes table (using an inner class approach for embedding attributes)."""

    id: int
    product_id: UUID
    attribute_name: str
    attribute_value: str


class Sale(BaseModel):
    """Schema for the sales table."""

    id: int
    datetime: datetime.datetime
    product_id: UUID
    location_id: UUID
    quantity: int


class Stock(BaseModel):
    """Schema for the historical stocks table."""

    date: datetime.date
    product_id: UUID
    location_id: UUID
    quantity: int


class Locations(BaseModel):
    locations: list[Location]

    @classmethod
    def from_csv(cls, csv_path: str) -> "Locations":
        df = pd.read_csv(csv_path)
        records = df.to_dict(orient="records")
        locations = [Location(**{str(k): v for k, v in row.items()}) for row in records]
        return cls(locations=locations)


class Products(BaseModel):
    products: list[Product]

    @classmethod
    def from_csv(cls, csv_path: str) -> "Products":
        df = pd.read_csv(csv_path)
        records = df.to_dict(orient="records")
        products = [Product(**{str(k): v for k, v in row.items()}) for row in records]
        return cls(products=products)


class Sales(BaseModel):
    sales: list[Sale]

    @classmethod
    def from_csv(cls, csv_path: str) -> "Sales":
        df = pd.read_csv(csv_path)
        records = df.to_dict(orient="records")
        sales = [Sale(**{str(k): v for k, v in row.items()}) for row in records]
        return cls(sales=sales)


class Stocks(BaseModel):
    stocks: list[Stock]

    @classmethod
    def from_csv(cls, csv_path: str) -> "Stocks":
        df = pd.read_csv(csv_path)
        records = df.to_dict(orient="records")
        stocks = [Stock(**{str(k): v for k, v in row.items()}) for row in records]
        return cls(stocks=stocks)


class WeatherData(BaseModel):
    location_id: UUID
    datetime: datetime.datetime
    temperature_2m: float | None
    precipitation: float | None
    sunshine_duration: float | None
    weather_code: int | None
