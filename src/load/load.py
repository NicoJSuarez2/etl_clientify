import pandas as pd
from sqlalchemy import create_engine
import dotenv
import os
import urllib.parse
import glob


def load_env_variables(logger):
    """Cargar variables de entorno necesarias para la conexión."""
    dotenv.load_dotenv()
    logger.info("✅ Variables de entorno cargadas desde .env")
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
        f"SERVER={db_config['server']};"
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

def load_parquet_to_sql(file_path, engine, logger):
    """Leer un archivo parquet y cargarlo como tabla SQL con full load."""
    df = pd.read_parquet(file_path)
    table_name = os.path.splitext(os.path.basename(file_path))[0]
    
    try:
        # Vaciar tabla existente (mantiene estructura y tipos)
        with engine.connect() as conn:
            conn.execute(f"TRUNCATE TABLE {table_name}")
            conn.commit()
            logger.info(f"Tabla '{table_name}' vaciada para full load.")
        
        # Cargar todos los datos
        df.to_sql(
            table_name,
            engine,
            if_exists="append",
            index=False,
            chunksize=5000
        )
        logger.info(f"Tabla '{table_name}' cargada correctamente ({len(df)} filas insertadas).")
    except Exception as e:
        logger.error(f"Error al cargar tabla '{table_name}': {str(e)}")
        raise

def ejecucion_carga(logger):
    # Cargar configuración y engine
    db_config = load_env_variables(logger)
    engine = create_db_engine(db_config)

    logger.info(f"DB_USERNAME: {db_config['username']}")
    logger.info(f"DB_PASSWORD: {'***' if db_config['password'] else None}")
    logger.info(f"DB_SERVER_IP: {db_config['server']}")
    logger.info(f"DB_DATABASE: {db_config['database']}")

    # Carpeta donde están los parquet
    parquet_files = get_parquet_files("data/stage/*.parquet")
    if not parquet_files:
        logger.info("No se encontraron archivos parquet en la ruta especificada.")
        return

    # Cargar todos los archivos como tablas
    for file in parquet_files:
        load_parquet_to_sql(file, engine, logger)

    logger.info("¡Todas las tablas han sido cargadas correctamente!")


