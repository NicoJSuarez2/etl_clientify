import pandas as pd
from sqlalchemy import create_engine
import dotenv
import os
import urllib
import glob

def load_env_variables():
    """Cargar variables de entorno necesarias para la conexión."""
    dotenv.load_dotenv()
    return {
        "server": os.getenv("DB_SERVER_IP"),
        "database": os.getenv("DB_DATABASE"),
        "username": os.getenv("DB_USERNAME"),
        "password": os.getenv("DB_PASSWORD")
    }

def create_db_engine(db_config):
    """Crear un engine de SQLAlchemy para SQL Server usando pyodbc."""
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={db_config['server']},1433;"
        f"DATABASE={db_config['database']};"
        f"UID={db_config['username']};"
        f"PWD={db_config['password']};"
        "Encrypt=no;"  # Cambiar a 'yes' si tu SQL Server usa TLS
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)
    return engine

def get_parquet_files(path_pattern):
    """Obtener todos los archivos parquet de una carpeta específica."""
    return glob.glob(path_pattern)

def load_parquet_to_sql(file_path, engine):
    """Leer un archivo parquet y cargarlo como tabla SQL con el mismo nombre."""
    df = pd.read_parquet(file_path)
    table_name = os.path.splitext(os.path.basename(file_path))[0]  # Nombre sin extensión
    df.to_sql(
        table_name,
        engine,
        if_exists="replace",
        index=False,
        chunksize=5000
    )
    print(f"Tabla '{table_name}' cargada correctamente.")

def main():
    # Cargar configuración y engine
    db_config = load_env_variables()
    engine = create_db_engine(db_config)

    print("DB_USERNAME:", db_config['username'])
    print("DB_PASSWORD:", "***" if db_config['password'] else None)
    print("DB_SERVER_IP:", db_config['server'])
    print("DB_DATABASE:", db_config['database'])

    # Carpeta donde están los parquet
    parquet_files = get_parquet_files("data/stage/*.parquet")
    if not parquet_files:
        print("No se encontraron archivos parquet en la ruta especificada.")
        return

    # Cargar todos los archivos como tablas
    for file in parquet_files:
        load_parquet_to_sql(file, engine)

    print("¡Todas las tablas han sido cargadas correctamente!")

if __name__ == "__main__":
    main()
