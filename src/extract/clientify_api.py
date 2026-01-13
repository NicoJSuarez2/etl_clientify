import requests
import pandas as pd
import os
import time
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
from pathlib import Path


def config():

    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    API_KEY = os.getenv("TOKEN_CLIENTIFY")
    BASE_URL = os.getenv("BASE_URL", "https://api.clientify.net/v1")

    headers = {"Authorization": f"Token {API_KEY}", "Content-Type": "application/json"}

    # Endpoints principales
    endpoints = {
        #"contacts": "/contacts/",
        #"companies": "/companies/",
        #"deals": "/deals/",
        #"calls": "/calls/",
        #"tasks": "/tasks/",
        #"users": "/users/",
        "pipelines_stages": "/deals/pipelines/stages/",
        "tasks/types": "/tasks/types/",
        #"deals_pipelines": "/deals/pipelines/",
    }
    return BASE_URL, headers, endpoints


def load_incremental_fecha(
    logger,
    ) -> str:
    """
    Esta funcion solo se deberia utilizar si se desea hacer
    una carga incremental basada en la ultima de ejecucion
    """

    archivo_fecha = "ultima_fecha.txt"
    fecha_desde = None

    if not fecha_desde:
        if os.path.exists(archivo_fecha):
            with open(archivo_fecha, "r") as f:
                fecha_desde = f.read().strip()
                fecha_desde = fecha_desde[:16]  # <-- quitar segundos
                logger.info(f"📅 Usando fecha desde archivo: {fecha_desde}")
        else:
            fecha_desde = "2024-01-01T00:00"
            logger.info(f"⚠ Usando fecha inicial: {fecha_desde}")

    params = {}
    params["created[gte]"] = fecha_desde  # <-- formato válido

    return fecha_desde


def fetch_data(
    logger,
    endpoint: str,
    full_load: bool = False,
) -> pd.DataFrame:
    """
    ESTA FUNCION NECESITA CORRECIONES EN LA CARGA INCREMENTAL
    """

    params = {}
    #params["created[gte]"] = load_incremental_fecha(logger)
    BASE_URL, headers, _ = config()
    url = f"{BASE_URL}{endpoint}"
    all_results = []
    page = 1

    while True:
        params_page = {"page": page, "page_size": 100}
        resp = requests.get(url, headers=headers, params=params_page)

        if resp.status_code != 200:
            logger.info(f"❌ Error {resp.status_code} en {endpoint}: {resp.text}")
            break

        data = resp.json()
        results = data.get("results", [])

        if not results:
            break

        all_results.extend(results)
        logger.info(f"📄 {endpoint} - Página {page}: {len(results)} registros")
        page += 1
        time.sleep(0.4)

    if not full_load:
        archivo_fecha = "ultima_fecha.txt"
        # Guardar nueva fecha sin segundos
        fecha_actual = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
        with open(archivo_fecha, "w") as f:
            f.write(fecha_actual)
        logger.info(f"💾 Fecha actual guardada: {fecha_actual}")

    return pd.json_normalize(all_results) if all_results else pd.DataFrame()


def listar_deals_id(logger) -> list:
    """
    Lee deals.csv y retorna los ID únicos
    """
    deals_csv = (
        Path(__file__).resolve().parent.parent.parent / "data" / "raw" / "deals.csv"
    )

    # Validar que exista
    if not deals_csv.exists():
        logger.warning(
            f"⚠️ No existe el archivo: {deals_csv.name}. Se retorna lista vacía."
        )
        return []

    try:
        df = pd.read_csv(deals_csv)
    except Exception as e:
        logger.error(f"❌ Error leyendo {deals_csv.name}: {e}")
        return []

    # Validar que tenga columna id
    if "id" not in df.columns:
        logger.warning(f"⚠️ El archivo {deals_csv.name} no contiene columna 'id'.")
        return []

    deal_ids = df["id"].dropna().unique().tolist()

    logger.info(f"📌 Se encontraron {len(deal_ids)} IDs")
    return deal_ids


def extraccion_tiempos(logger) -> pd.DataFrame:
    """
    Extrae los tiempos de los deals usando sus IDs.
    Retorna un DataFrame con los datos.
    """
    BASE_URL = os.getenv("BASE_URL", "https://api.clientify.net/v1")
    _, headers, _ = config()

    deal_ids = listar_deals_id(logger)

    # --- Manejo cuando no hay deals id --- #
    if not deal_ids:
        logger.warning(
            "⚠️ No hay IDs de deals para procesar. Se retorna DataFrame vacío."
        )
        return pd.DataFrame()

    all_results = []
    ids_no_encontrados = []

    for deal_id in deal_ids:
        url = f"{BASE_URL}/deals/{deal_id}"
        resp = requests.get(url, headers=headers)
        logger.info(f"Fetching {url}")

        # --- Manejo de errores HTTP --- #
        if resp.status_code == 404:
            logger.info(f"⚠️ Deal {deal_id} no existe (404). Saltando...")
            ids_no_encontrados.append(deal_id)
            continue

        if resp.status_code != 200:
            logger.info(f"❌ Error {resp.status_code} en deal {deal_id}: {resp.text}")
            continue

        data = resp.json()

        # --- Si la respuesta es dict (un solo objeto) --- #
        if isinstance(data, dict):
            data["deal_id"] = deal_id
            all_results.append(data)
            continue

        # --- Si devuelve lista con `results` --- #
        if isinstance(data, dict) and "results" in data:
            for r in data["results"]:
                r["deal_id"] = deal_id
                all_results.append(r)

        time.sleep(0.5)

    df = pd.json_normalize(all_results) if all_results else pd.DataFrame()

    logger.info(f"\n⛔ Deals no encontrados (404): {ids_no_encontrados}")
    logger.info(f"📌 Registros totales: {len(df)}")

    return df
