import os
import pandas as pd
import uuid
import numpy as np
import time
from data_eng.settings import settings
from loguru import logger


def setup_folders(result_folder_path: str, cache_folder_path: str) -> None:
    """Ensure that result and cache folders exist."""

    os.makedirs(result_folder_path, exist_ok=True)
    os.makedirs(cache_folder_path, exist_ok=True)
    logger.debug(
        f"Result folder at {result_folder_path} and cache folder at {cache_folder_path} are set up."
    )


def locations_pipeline(
    locations_csv_path: str,
    result_folder_path: str,
    location_filename: str,
    location_uuid_map_path: str,
) -> None:
    """Pipeline to transform locations data. This create the locations result table."""
    # Load locations data
    locations_df: pd.DataFrame = pd.read_csv(locations_csv_path)
    logger.debug(f"Locations data loaded with shape: {locations_df.shape}")

    # Generate UUIDs for locations and reformat the table
    # No need for complicated cleaning here as data are 13 rows only and clean.
    locations_df["id"] = [str(uuid.uuid4()) for _ in range(len(locations_df))]
    locations_df.rename(columns={"city": "name"}, inplace=True)

    # Save the transformed locations table
    locations_df[["id", "name", "latitude", "longitude"]].to_csv(
        os.path.join(result_folder_path, location_filename), index=False
    )
    logger.success(
        f"Locations table saved to {os.path.join(result_folder_path, location_filename)}"
    )

    # Save the mapping of original location names to UUID in cache folder in json format
    location_uuid_map = dict(zip(locations_df["location_id"], locations_df["id"]))
    pd.Series(location_uuid_map).to_json(location_uuid_map_path)
    logger.success(f"Location UUID map saved to {location_uuid_map_path}")


def products_pipeline(
    products_csv_path: str,
    result_folder_path: str,
    cache_folder_path: str,
    products_filename: str,
    product_attributes_filename: str,
) -> None:
    """Pipeline to transform products data. This create the products result table and the product attributes table."""
    # Load products data
    products_df: pd.DataFrame = pd.read_csv(products_csv_path)
    logger.debug(f"Products data loaded with shape: {products_df.shape}")

    # Generate UUIDs for products and reformat the table
    products_df["id"] = [str(uuid.uuid4()) for _ in range(len(products_df))]
    # Convert perishability to boolean
    products_df["is_perishable"] = products_df["perishability"].apply(
        lambda x: str(x).strip().capitalize() == "PERISHABLE"
    )
    products_df.drop(columns=["perishability"], inplace=True)

    # Save a csv file with columns id, is_perishable and category only
    products_df[["id", "category", "is_perishable"]].to_csv(
        os.path.join(result_folder_path, products_filename), index=False
    )

    # Build product_attributes table
    # Gather attribute columns: everything except the product-level columns we keep in products.csv
    attribute_columns: list[str] = [
        column_name
        for column_name in products_df.columns
        if column_name not in ("id", "category", "is_perishable")
    ]
    # Prepare long-format table of attributes
    products_df["product_id"] = products_df["id"]

    # see https://pandas.pydata.org/docs/reference/api/pandas.melt.html
    product_attributes_df: pd.DataFrame = pd.melt(
        products_df,
        id_vars=["product_id"],
        value_vars=attribute_columns,
        var_name="attribute_name",
        value_name="attribute_value",
    )

    # Add an integer id and save
    product_attributes_df["id"] = np.arange(1, len(product_attributes_df) + 1)
    product_attributes_df[
        ["id", "product_id", "attribute_name", "attribute_value"]
    ].to_csv(os.path.join(result_folder_path, product_attributes_filename), index=False)
    logger.success(
        f"Product attributes table saved to {os.path.join(result_folder_path, product_attributes_filename)}"
    )

    # Save the mapping of original sku to UUID in cache folder in json format
    sku_uuid_map = dict(zip(products_df["sku"], products_df["id"]))
    pd.Series(sku_uuid_map).to_json(
        os.path.join(cache_folder_path, "product_uuid_map.json")
    )
    logger.success(
        f"Product UUID map saved to {os.path.join(cache_folder_path, 'product_uuid_map.json')}"
    )


def sales_pipeline(
    sales_csv_path: str,
    result_folder_path: str,
    sales_filename: str,
    location_uuid_map_path: str,
    product_uuid_map_path: str,
) -> None:
    """Pipeline to transform sales data. This create the sales result table."""
    # Load mapping of original sku to product UUID
    product_uuid_map: dict[str, str] = pd.read_json(
        product_uuid_map_path, typ="series"
    ).to_dict()
    # Load mapping of original location city abbreviation to location UUID
    location_uuid_map: dict[str, str] = pd.read_json(
        location_uuid_map_path, typ="series"
    ).to_dict()

    # Load sales data
    sales_df: pd.DataFrame = pd.read_csv(sales_csv_path)
    logger.debug(f"Sales data loaded with shape: {sales_df.shape}")

    # Map original sku and location_id to UUIDs using provided maps
    sales_df["product_id"] = sales_df["sku"].map(product_uuid_map)
    sales_df["location_id"] = sales_df["location_id"].map(location_uuid_map)

    # Rename columns and select final columns
    sales_df["id"] = np.arange(1, len(sales_df) + 1)
    sales_df.rename(
        columns={"date": "datetime", "original_quantity": "quantity"}, inplace=True
    )

    # Save the transformed sales table
    sales_df[["id", "datetime", "product_id", "location_id", "quantity"]].to_csv(
        os.path.join(result_folder_path, sales_filename), index=False
    )
    logger.success(
        f"Sales table saved to {os.path.join(result_folder_path, sales_filename)}"
    )


def stocks_pipeline(
    stocks_csv_path: str,
    result_folder_path: str,
    stocks_filename: str,
    location_uuid_map_path: str,
    product_uuid_map_path: str,
) -> None:
    """Pipeline to transform stocks data. This create the stocks result table."""

    # Load mapping of original sku to product UUID
    product_uuid_map: dict[str, str] = pd.read_json(
        product_uuid_map_path, typ="series"
    ).to_dict()
    # Load mapping of original location city abbreviation to location UUID
    location_uuid_map: dict[str, str] = pd.read_json(
        location_uuid_map_path, typ="series"
    ).to_dict()

    # Load stocks data
    stocks_df: pd.DataFrame = pd.read_csv(stocks_csv_path)
    logger.debug(f"Stocks data loaded with shape: {stocks_df.shape}")

    # Map original sku and location_id to UUIDs using provided maps
    stocks_df["product_id"] = stocks_df["sku"].map(product_uuid_map)
    stocks_df["location_id"] = stocks_df["location_id"].map(location_uuid_map)

    stocks_df["date"] = pd.to_datetime(stocks_df["date_at"]).dt.date
    stocks_df.rename(columns={"available_quantity": "quantity"}, inplace=True)

    # Save the transformed historical_stocks table
    stocks_df[["date", "product_id", "location_id", "quantity"]].to_csv(
        os.path.join(result_folder_path, stocks_filename), index=False
    )
    logger.success(
        f"Historical stocks table saved to {os.path.join(result_folder_path, stocks_filename)}"
    )


if __name__ == "__main__":
    setup_folders(
        result_folder_path=settings.RESULT_DIR, cache_folder_path=settings.CACHE_DIR
    )
    # Run the pipelines sequentially
    logger.info("Starting locations pipelines...")
    time_begin = time.time()
    locations_pipeline(
        locations_csv_path=settings.LOCATIONS_CSV_PATH,
        location_filename=settings.location_filename,
        result_folder_path=settings.RESULT_DIR,
        location_uuid_map_path=settings.LOCATION_UUID_MAP_PATH,
    )
    logger.success(
        f"Locations pipeline completed in {time.time() - time_begin:.2f} seconds."
    )
    logger.info("Starting products pipelines...")
    time_begin = time.time()
    products_pipeline(
        products_csv_path=settings.PRODUCTS_CSV_PATH,
        products_filename=settings.products_filename,
        product_attributes_filename=settings.PRODUCTS_ATTRIBUTES_FILENAME,
        result_folder_path=settings.RESULT_DIR,
        cache_folder_path=settings.CACHE_DIR,
    )
    logger.success(
        f"Products pipeline completed in {time.time() - time_begin:.2f} seconds."
    )
    logger.info("Starting sales pipelines...")
    time_begin = time.time()
    sales_pipeline(
        sales_csv_path=settings.SALES_CSV_PATH,
        result_folder_path=settings.RESULT_DIR,
        location_uuid_map_path=settings.LOCATION_UUID_MAP_PATH,
        product_uuid_map_path=settings.PRODUCT_UUID_MAP_PATH,
        sales_filename=settings.sales_filename,
    )

    logger.info("Starting stocks pipelines...")
    time_begin = time.time()
    stocks_pipeline(
        stocks_csv_path=settings.STOCKS_CSV_PATH,
        result_folder_path=settings.RESULT_DIR,
        stocks_filename=settings.stocks_filename,
        location_uuid_map_path=settings.LOCATION_UUID_MAP_PATH,
        product_uuid_map_path=settings.PRODUCT_UUID_MAP_PATH,
    )
    logger.success(
        f"Stocks pipeline completed in {time.time() - time_begin:.2f} seconds."
    )
    logger.success("All pipelines completed successfully.")
