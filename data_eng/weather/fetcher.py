import datetime
import os
from typing import cast
from uuid import UUID
import openmeteo_requests  # type: ignore
import pandas as pd
import requests_cache
from retry_requests import retry  # type: ignore
from data_eng.models import Locations, WeatherData
from data_eng.settings import settings
from loguru import logger

#
START_DATE = "2023-01-01"
END_DATE = "2023-02-07"


def fetch_and_save_weather_data(
    start_date: str, end_date: str, locations_csv_path: str, output_csv_path: str
) -> None:
    """
    Fetches historical hourly weather data for multiple variables using the high-resolution
    ECMWF IFS model and saves it to a CSV.
    """

    locations: Locations = Locations.from_csv(locations_csv_path)
    city_coordinates: dict[str, tuple[float, float]] = {}
    for location in locations.locations:
        city_coordinates[location.name] = (location.latitude, location.longitude)

    # Map city name back to location ID
    name_to_id = {loc.name: loc.id for loc in locations.locations}

    latitudes = ",".join(str(coord[0]) for coord in city_coordinates.values())
    longitudes = ",".join(str(coord[1]) for coord in city_coordinates.values())
    cities = list(city_coordinates.keys())

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)  # type: ignore

    URL = "https://archive-api.open-meteo.com/v1/archive"

    hourly_variables = [
        "temperature_2m",
        "precipitation",
        "sunshine_duration",
        "weather_code",
    ]

    params = {
        "latitude": latitudes,
        "longitude": longitudes,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": hourly_variables,
        "timezone": "auto",
        "models": "ecmwf_ifs",
    }

    logger.info(
        f"Fetching historical weather data using ECMWF IFS model for {len(cities)} cities..."
    )
    logger.info(f"Variables: {hourly_variables}")

    responses = openmeteo.weather_api(URL, params=params)

    all_weather_records = []

    for i, response in enumerate(responses):
        city_name = cities[i]
        location_id = name_to_id[city_name]

        hourly = response.Hourly()

        if hourly is None:
            logger.warning(f"No hourly data returned for city: {city_name}, skipping.")
            continue

        time_index_utc = pd.to_datetime(hourly.Time(), unit="s", utc=True)
        logger.debug(f"time_index_utc sample: {time_index_utc}")

        temp_var = hourly.Variables(0)
        precip_var = hourly.Variables(1)
        sunshine_var = hourly.Variables(2)
        weather_code_var = hourly.Variables(3)

        if any(
            var is None
            for var in [temp_var, precip_var, sunshine_var, weather_code_var]
        ):
            logger.warning(f"Missing variable data for city: {city_name}, skipping.")
            continue

        temp_data = temp_var.ValuesAsNumpy()  # type: ignore
        precip_data = precip_var.ValuesAsNumpy()  # type: ignore
        sunshine_data = sunshine_var.ValuesAsNumpy()  # type: ignore
        weather_code_data = weather_code_var.ValuesAsNumpy()  # type: ignore

        city_df = pd.DataFrame(
            {
                "datetime_utc": time_index_utc,
                "location_id": str(location_id),
                "temperature_2m": temp_data,
                "precipitation": precip_data,
                "sunshine_duration": sunshine_data,
                "weather_code": weather_code_data,
            }
        )

        city_df["datetime"] = city_df["datetime_utc"]

        records = city_df[
            [
                "location_id",
                "datetime",
                "temperature_2m",
                "precipitation",
                "sunshine_duration",
                "weather_code",
            ]
        ].to_dict(orient="records")

        validated_records: list[WeatherData] = []
        for record in records:
            temp_val = cast(float | None, record.get("temperature_2m"))
            precip_val = cast(float | None, record.get("precipitation"))
            sunshine_val = cast(float | None, record.get("sunshine_duration"))
            weather_code_val = cast(int | None, record.get("weather_code"))

            validated = WeatherData(
                location_id=UUID(record["location_id"]),
                datetime=cast(datetime.datetime, record["datetime"]),
                temperature_2m=float(temp_val)
                if temp_val is not None and not pd.isna(temp_val)
                else None,
                precipitation=float(precip_val)
                if precip_val is not None and not pd.isna(precip_val)
                else None,
                sunshine_duration=float(sunshine_val)
                if sunshine_val is not None and not pd.isna(sunshine_val)
                else None,
                weather_code=int(weather_code_val)
                if weather_code_val is not None and not pd.isna(weather_code_val)
                else None,
            )
            validated_records.append(validated)

        all_weather_records.extend([r.model_dump() for r in validated_records])

    final_weather_df = pd.DataFrame(all_weather_records)
    final_weather_df.to_csv(
        output_csv_path, index=False, date_format="%Y-%m-%d %H:%M:%S"
    )

    logger.success(
        f"Successfully saved {len(final_weather_df)} weather records to {output_csv_path}"
    )


if __name__ == "__main__":
    WEATHER_CSV_PATH = os.path.join(settings.RESULT_DIR, settings.WEATHER_FILENAME)
    LOCATIONS_CSV_PATH = os.path.join(settings.RESULT_DIR, settings.location_filename)

    fetch_and_save_weather_data(
        start_date=START_DATE,
        end_date=END_DATE,
        locations_csv_path=LOCATIONS_CSV_PATH,
        output_csv_path=WEATHER_CSV_PATH,
    )
