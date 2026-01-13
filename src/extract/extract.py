# src/extract.py
from src.extract.clientify_api import *


def extract_all(logger, full_load: bool = True) -> dict:
    """
    Extrae datasets desde Clientify y archivos procesados.
    Devuelve un diccionario con {nombre_dataset: DataFrame}.
    """
    data = {}
    # ===============================
    # === 1 Descargar endpoints generales ===
    # ===============================
    _, _, endpoints = config()

    for name, endpoint in endpoints.items():
        try:
            df = fetch_data(logger, endpoint, full_load)
            if df is not None and not df.empty:
                data[name] = df
        except Exception as e:
            logger.info(f"⚠️ No se pudo extraer {name}: {e}")

    return data

def extract_stream(logger, full_load: bool = True):
    """
    Extrae datasets uno por uno desde Clientify.
    Generador que produce (nombre_dataset, DataFrame).
    """
    _, _, endpoints = config()

    for name, endpoint in endpoints.items():
        try:
            logger.info(f"📡 Extrayendo {name}...")
            df = fetch_data(logger, endpoint, full_load)

            if df is None or df.empty:
                logger.info(f"⚠️ {name} vacío, se omite.")
                continue

            yield name, df   # 👈 devuelve uno por uno

        except Exception as e:
            logger.info(f"⚠️ No se pudo extraer {name}: {e}")