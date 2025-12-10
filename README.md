# Take-Home-Metreecs

Data engineering coding test by metreecs. The instructions can be found in the pdf file `Take home assignment`

## Requirements

1. This project use uv as python package manager.
   Please install it from:

```bash
https://docs.astral.sh/uv/getting-started/installation/
```

2. This project needs data provided from the company Metreecs. Please download the data folder following the company instruction. Extract the .zip file in the project root. File paths should corresponds to the path in data_eng/settings.py. If not please edit the constants in this file.

## Part 1

To run the transformation pipeline, run in the root_folder:

```bash
uv run python -m data_eng.preparation.pipeline
```

You will find the results in data/results.

## Part 2

This part consist on fetching weather data from the public [Open-Meteo API](https://open-meteo.com/en/docs/historical-weather-api).
Run the following command to do so in the root folder:

```bash
uv run python -m data_eng.weather.fetcher
```

The results are stored in data/results/weather.csv
The times data inside the hourly variable are not correctly fetched:

```json
{
    "latitude": 52.52,
    "longitude": 13.419,
    "elevation": 44.812,
    "generationtime_ms": 2.2119,
    "utc_offset_seconds": 0,
    "timezone": "Europe/Berlin",
    "timezone_abbreviation": "CEST",
    "hourly": {
        "time": ["2022-07-01T00:00", "2022-07-01T01:00", "2022-07-01T02:00", ...],
        "temperature_2m": [13, 12.7, 12.7, 12.5, 12.5, 12.8, 13, 12.9, 13.3, ...]
    },
    "hourly_units": {
        "temperature_2m": "°C"
    }
}
```

Indeed, we have the same date for all rows.
