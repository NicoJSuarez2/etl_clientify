#%%
import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=master;"
    "UID=x;"
    "PWD=x;"
    "Encrypt=no;"
)

print("Conectado correctamente")




#%%
import pandas as pd
from sqlalchemy import create_engine
import dotenv
import os
from sqlalchemy import create_engine

dotenv.load_dotenv()
# Conexión
server_ip = os.getenv("server_ip")
database = os.getenv("database")
username = os.getenv("username")
password = os.getenv("password")
engine = create_engine(
    "mssql+pyodbc://sa:TuPasswordFuerte123!@localhost:1433/master"
    "?driver=ODBC+Driver+18+for+SQL+Server"
    "&Encrypt=no"
)

# Leer parquet
df = pd.read_parquet("data/stage/calls.parquet")

# Cargar a SQL Server
df.to_sql(
    "tabla_destino",
    engine,
    if_exists="replace",   # crea tabla si no existe
    index=False,
    chunksize=5000
)
