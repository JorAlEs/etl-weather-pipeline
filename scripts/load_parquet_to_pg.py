import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_DB   = os.getenv("PG_DB")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")

# Ruta absoluta a la carpeta data/parquet
parquet_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "parquet"))
parquet_files = sorted([f for f in os.listdir(parquet_dir) if f.endswith(".parquet")])
if not parquet_files:
    raise FileNotFoundError("❌ No Parquet files found in data/parquet.")

latest_file = parquet_files[-1]
df = pd.read_parquet(os.path.join(parquet_dir, latest_file))

# Conexión
engine = create_engine(f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")
df.to_sql("weather_forecast", engine, if_exists="append", index=False)
print(f"✅ Datos cargados a PostgreSQL desde {latest_file}")
# Cierre de conexión
engine.dispose()    