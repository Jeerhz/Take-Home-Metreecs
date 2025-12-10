import datetime
import os
from typing import cast
import numpy as np
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

        # We verified that these are not None above
        temp_data: np.ndarray = temp_var.ValuesAsNumpy()  # type: ignore
        precip_data: np.ndarray = precip_var.ValuesAsNumpy()  # type: ignore
        sunshine_data: np.ndarray = sunshine_var.ValuesAsNumpy()  # type: ignore
        weather_code_data: np.ndarray = weather_code_var.ValuesAsNumpy()  # type: ignore

        city_df = pd.DataFrame(
            {
                "datetime": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left",
                ),
                "location_id": str(location_id),
                "temperature_2m": temp_data,
                "precipitation": precip_data,
                "sunshine_duration": sunshine_data,
                "weather_code": weather_code_data,
            }
        )

        # We convert the dataframe to a dict and then validate each record using Pydantic
        records: list[
            dict[str, datetime.datetime | UUID | str | float | int | None]
        ] = cast(
            list[dict[str, datetime.datetime | UUID | str | float | int | None]],
            city_df[
                [
                    "location_id",
                    "datetime",
                    "temperature_2m",
                    "precipitation",
                    "sunshine_duration",
                    "weather_code",
                ]
            ].to_dict(orient="records"),
        )

        validated_records: list[WeatherData] = []
        for record in records:
            validated = WeatherData(**record)  # type: ignore
            validated_records.append(validated)

        all_weather_records.extend([r.model_dump() for r in validated_records])

    # Save all weather records to CSV using pandas
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
