# Take-Home-Metreecs

Data engineering coding test by metreecs. The instructions can be found in the pdf file `Take home assignment`

## Requirements

1. This project use uv as python package manager.
   Please install it from:

```bash
https://docs.astral.sh/uv/getting-started/installation/
```

Then you can create virtual environment and install dependencies by running in project root folder:

```bash
uv venv .venv
uv sync
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
