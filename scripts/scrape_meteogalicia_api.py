import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import logging
from sqlalchemy import create_engine, text

# Paths
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
dotenv_path = os.path.join(root_dir, ".env")
parquet_dir = os.path.join(root_dir, "data", "parquet")

# Cargar .env
load_dotenv(dotenv_path)
API_KEY = os.getenv("METEOGALICIA_API_KEY")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_DB   = os.getenv("PG_DB")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def get_forecast_data():
    params = {
        "coords": "-8.544844,42.880447",
        "API_KEY": API_KEY,
        "lang": "es",
        "format": "application/json",
        "tz": "Europe/Madrid"
    }
    response = requests.get("https://servizos.meteogalicia.gal/apiv4/getNumericForecastInfo", params=params)
    response.raise_for_status()
    return response.json()

def process_data(json_data):
    records = []
    for feature in json_data.get("features", []):
        lon, lat = feature["geometry"]["coordinates"]
        for day in feature["properties"].get("days", []):
            for var in day.get("variables", []):
                name = var.get("name")
                model = var.get("model")
                grid = var.get("grid")
                unit = var.get("units", None)
                for val in var.get("values", []):
                    value = val.get("value") or val.get("moduleValue")
                    records.append({
                        "datetime": val.get("timeInstant"),
                        "variable": name,
                        "value": str(value),
                        "model": model,
                        "grid": grid,
                        "unit": unit,
                        "lon": lon,
                        "lat": lat
                    })

    df = pd.DataFrame(records)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df

def save_parquet(df):
    today_str = datetime.today().strftime("%Y-%m-%d")
    os.makedirs(parquet_dir, exist_ok=True)
    file_path = os.path.join(parquet_dir, f"santiago_api_weather_{today_str}.parquet")
    df.to_parquet(file_path, index=False)
    logging.info(f"💾 Guardado archivo Parquet: {file_path}")
    return file_path

def insert_to_postgresql(df):
    engine = create_engine(f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")
    with engine.begin() as conn:
        for _, row in df.iterrows():
            insert_stmt = text("""
                INSERT INTO weather_forecast (datetime, variable, value, model, grid, unit, lon, lat)
                VALUES (:datetime, :variable, :value, :model, :grid, :unit, :lon, :lat)
                ON CONFLICT (datetime, variable) DO NOTHING
            """)
            conn.execute(insert_stmt, row.to_dict())
    logging.info("✅ Datos insertados en PostgreSQL sin duplicados.")

if __name__ == "__main__":
    try:
        logging.info("🔄 Descargando datos desde MeteoGalicia...")
        data = get_forecast_data()
        df = process_data(data)
        print(df.head())
        file_path = save_parquet(df)
        insert_to_postgresql(df)
    except Exception as e:
        logging.error(f"❌ Error: {e}")
        raise
    finally:
        logging.info("🔚 Proceso finalizado.")