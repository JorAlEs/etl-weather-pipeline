import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import logging

# Load .env with METEOGALICIA_API_KEY
load_dotenv()
API_KEY = os.getenv("METEOGALICIA_API_KEY")

# API configuration
BASE_URL = "https://servizos.meteogalicia.gal/apiv4/getNumericForecastInfo"
COORDS = "-8.544844,42.880447"  # Santiago de Compostela
TODAY = datetime.today().strftime("%Y-%m-%dT00:00:00")

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def get_forecast_data():
    params = {
        "coords": COORDS,
        "API_KEY": API_KEY,
        "lang": "es",
        "format": "application/json",
        "tz": "Europe/Madrid"
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()

def process_data(json_data):
    records = []

    for feature in json_data.get("features", []):
        location = feature["geometry"]["coordinates"]
        properties = feature.get("properties", {})
        days = properties.get("days", [])

        for day in days:
            variables = day.get("variables", [])
            for var in variables:
                name = var.get("name")
                model = var.get("model")
                grid = var.get("grid")
                unit = var.get("units", None)
                values = var.get("values", [])
                for val in values:
                    records.append({
                        "datetime": val.get("timeInstant"),
                        "variable": name,
                        "value": val.get("value") or val.get("moduleValue"),
                        "model": model,
                        "grid": grid,
                        "unit": unit,
                        "lon": location[0],
                        "lat": location[1]
                    })

    df = pd.DataFrame(records)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df

def save_as_parquet(df):
    today_str = datetime.today().strftime("%Y-%m-%d")
    output_dir = "data/parquet"
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(output_dir, f"santiago_api_weather_{today_str}.parquet")
    df.to_parquet(output_file, index=False)
    logging.info(f"💾 Saved Parquet file: {output_file}")

if __name__ == "__main__":
    try:
        logging.info("🔄 Fetching forecast data from MeteoGalicia API...")
        raw_data = get_forecast_data()
        df_forecast = process_data(raw_data)
        print(df_forecast.head())
        save_as_parquet(df_forecast)
    except Exception as e:
        logging.error(f"❌ Error fetching or processing data: {e}")
