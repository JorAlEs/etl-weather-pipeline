from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import pandas as pd
import os

URL = "https://www.meteogalicia.gal/datosred/infoweb/meteo/observacion/estacions/estacionsDetalle.action?idEst=1429&request_locale=es"

options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(options=options)
driver.get(URL)

# Espera que cargue la tabla
time.sleep(5)

# Encuentra la tabla de datos
tabla = driver.find_element(By.ID, "tbMedidas")

# Extrae el contenido
filas = tabla.find_elements(By.TAG_NAME, "tr")
data = []
for fila in filas[1:]:
    celdas = fila.find_elements(By.TAG_NAME, "td")
    data.append([celda.text for celda in celdas])

driver.quit()

# Procesa con pandas
df = pd.DataFrame(data, columns=[
    "fecha", "hora", "temp", "humedad", "punto_rocio", "presion",
    "viento_direccion", "viento_vel", "lluvia", "solar"
])

# Limpieza
df["fecha_hora"] = df["fecha"] + " " + df["hora"]
df.drop(columns=["hora", "punto_rocio", "presion", "viento_direccion", "solar"], inplace=True)

# Guardar CSV
os.makedirs("data/raw", exist_ok=True)
df.to_csv("data/raw/santiago_real_weather.csv", index=False)
print("✅ CSV generado con datos reales.")
