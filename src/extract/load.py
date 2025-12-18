# src/load.py
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables desde .env
from pathlib import Path as P

root = Path(__file__).resolve().parent.parent.parent

# Carpeta de salida configurable
DATA_DIR = root / "data" / "raw"


def load_to_csv(logger, df: pd.DataFrame, name: str, folder: Path = DATA_DIR):
    """
    Guarda un DataFrame en formato CSV dentro de la carpeta de process.
    """
    if df is not None and not df.empty:
        Path(folder).mkdir(parents=True, exist_ok=True)
        file_path = f"{folder}/{name}.csv"
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        logger.info(f"✅ Guardado en CSV: {file_path} ({len(df)} registros)")
    else:
        logger.info(f"⚠️ DataFrame vacío: {name}")
