# src/extract.py
from src.extract.clientify_api import *


def extract_all(logger, full_load: bool = False) -> dict:
    """
    Extrae datasets desde Clientify y archivos procesados.
    Devuelve un diccionario con {nombre_dataset: DataFrame}.
    """
    data = {}
    # ===============================
    # === 1 Descargar endpoints generales ===
    # ===============================
    _, _, endpoints = config(logger)

    for name, endpoint in endpoints.items():
        try:
            df = fetch_data(logger, endpoint, full_load=full_load)
            if df is not None and not df.empty:
                data[name] = df
        except Exception as e:
            logger.info(f"⚠️ No se pudo extraer {name}: {e}")

    return data
