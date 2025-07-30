
import os
import logging
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

URL = "https://www.meteogalicia.gal/datosred/infoweb/meteo/observacion/estacions/estacionsDetalle.action?idEst=1429&request_locale=es"

def scrape_meteogalicia():
    logging.info("Starting browser in headless mode.")
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)

    try:
        logging.info(f"Accessing {URL}")
        driver.get(URL)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "tbMedidas"))
        )

        tabla = driver.find_element(By.ID, "tbMedidas")
        filas = tabla.find_elements(By.TAG_NAME, "tr")

        data = []
        for fila in filas[1:]:
            celdas = fila.find_elements(By.TAG_NAME, "td")
            if len(celdas) == 10:
                data.append([celda.text.strip() for celda in celdas])

        df = pd.DataFrame(data, columns=[
            "fecha", "hora", "temp", "humedad", "punto_rocio", "presion",
            "viento_direccion", "viento_vel", "lluvia", "solar"
        ])
        df["fecha_hora"] = pd.to_datetime(df["fecha"] + " " + df["hora"], errors='coerce')
        df.drop(columns=["hora", "punto_rocio", "presion", "viento_direccion", "solar"], inplace=True)
        df = df.dropna(subset=["fecha_hora"])

        return df

    except Exception as e:
        logging.error(f"Scraping failed: {e}")
        return pd.DataFrame()
    finally:
        driver.quit()

def save_daily_data(df):
    today_str = datetime.today().strftime("%Y-%m-%d")
    output_dir = "data/raw"
    os.makedirs(output_dir, exist_ok=True)

    # Save unique daily snapshot
    daily_file = os.path.join(output_dir, f"santiago_weather_{today_str}.csv")
    df.to_csv(daily_file, index=False)

    # Append to master dataset
    master_file = os.path.join(output_dir, "santiago_weather_master.csv")
    if os.path.exists(master_file):
        df_existing = pd.read_csv(master_file)
        df_combined = pd.concat([df_existing, df], ignore_index=True)
        df_combined.drop_duplicates(subset=["fecha_hora"], inplace=True)
    else:
        df_combined = df

    df_combined.to_csv(master_file, index=False)
    logging.info(f"Data saved: daily snapshot and master dataset updated.")

if __name__ == "__main__":
    df_weather = scrape_meteogalicia()
    if not df_weather.empty:
        save_daily_data(df_weather)
        logging.info("✅ Weather data collected and stored successfully.")
    else:
        logging.warning("⚠️ No data scraped.")