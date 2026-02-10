import ast
import pandas as pd
from pathlib import Path
from pathlib import Path
import logging
from typing import Optional, Tuple


def config_logger():
    logger = logging.getLogger("clientify_etl")

    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        # === Ruta raíz del proyecto === #
        root_path = (
            Path(__file__).resolve().parent.parent.parent
        )  # sube 3  niveles (src → project root)

        # === Carpeta log === #
        logs_path = root_path / "log"
        logs_path.mkdir(parents=True, exist_ok=True)

        log_file = logs_path / "etl.log"

        # === Handler para archivo === #
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

        # === Handler para consola === #
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)

        logger.info(f"📁 Archivos log en: {log_file}")

    return logger


# =============================
# CONFIGURACIÓN DE COLUMNAS
# =============================
root = Path(__file__).resolve().parent.parent.parent
COLUMNAS_ELIMINAR = {
    "tasks": ["additional_option", "location", "guest_users", "tags", "type_desc"],
    "deals": ["company_name", "expected_closed_date_hora", "actual_closed_date_hora"],
    "companies": ["picture_url", "facebook_url", "linkedin_url", "twitter_url"],
    "calls": [
        "call_recording",
        "call_direction",
        "call_type",
        "call_medium",
        "call_source",
    ],
}

COLUMNAS_NUMERICAS = {
    "tasks": ["deals", "task_type", "task_stage", "related_companies"],
    "deals": ["contact", "company"],
    "calls": [
        "audio_url",
        "integration_id",
        "related_companies",
        "related_deals",
        "related_contacts",
    ],
}

COLUMNAS_FECHA_HORA = {
    "tasks": [
        "start_datetime",
        "end_datetime",
        "due_date",
        "created_at",
        "created",
        "modified",
        "completed_date",
    ],
    "deals": ["created_at", "modified_at", "close_date"],
    "calls": ["register_date", "modified_at", "call_time"],
    "companies": ["last_viewed", "last_interaction", "created", "modified"],
}


# =============================
# FUNCIONES BASE
# =============================


def load_data(path: Path) -> pd.DataFrame:
    """Carga un CSV en un DataFrame."""
    return pd.read_csv(path)


def limpiar_columna_numeros(df: pd.DataFrame, columnas: list) -> pd.DataFrame:
    """Limpia las columnas numéricas, eliminando caracteres no numéricos."""
    for col in columnas:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"[^0-9/]", "", regex=True)
                .str.replace("/", "", regex=False)
                .str.slice(1)
                .replace("", pd.NA)
            )
    return df


def separar_fecha_hora(df: pd.DataFrame, columnas: list) -> pd.DataFrame:
    """Separa columnas datetime en dos: <col>_fecha y <col>_hora."""
    for col in columnas:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            df[f"{col}_fecha"] = df[col].dt.date
            df[f"{col}_hora"] = df[col].dt.time
            df = df.drop(columns=[col])
    return df


def eliminar_urls(df: pd.DataFrame, nombre: str) -> pd.DataFrame:
    """
    Recorre todas las columnas del DataFrame.
    Si el primer valor de una columna comienza con 'http' o 'https',
    limpia esa columna:
    - Mantiene solo números
    - Elimina el primer carácter (por ejemplo, un número basura)
    """
    if nombre == "users":
        return df

    for col in df.columns:
        primer_valor = str(df[col].iloc[0]).lower()

        if primer_valor.startswith("http"):

            # Dejar solo números en toda la columna
            df[col] = df[col].astype(str).str.replace(r"[^0-9]", "", regex=True)

            # Eliminar solo el segundo carácter por que nose
            df[col] = df[col].str[1:]

    return df


def desanidar_columna(df: pd.DataFrame, columna: str) -> pd.DataFrame:
    """
    # Limpieza específica para deals
    Desanida una columna que contiene listas de diccionarios con claves 'field' y 'value'.

    Parámetros:
    df (pd.DataFrame): DataFrame original.
    columna (str): Nombre de la columna que contiene la lista de diccionarios.

    Retorna:
    pd.DataFrame: DataFrame con las columnas desanidadas.
    """
    # Convertir la columna a listas de diccionarios (si está en formato string)
    df = df.copy()
    df[columna] = df[columna].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) else x
    )

    # Expandir cada fila en un diccionario plano
    filas_expandidas = []
    for lista_diccionarios in df[columna]:
        fila = {}
        for item in lista_diccionarios:
            valor = item.get("value")
            # Si el valor es lista, convertir a string separado por comas
            if isinstance(valor, list):
                valor = ", ".join(valor)
            fila[item.get("field")] = valor
        filas_expandidas.append(fila)

    # Crear DataFrame final
    df_desanidado = pd.DataFrame(filas_expandidas)

    # Mantener el índice original para poder unir si se necesita
    df_desanidado.index = df.index

    return df_desanidado



def custom_columns(
    logger,
    df: pd.DataFrame,
    nombre: str
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """
    Aplica limpiezas específicas según el nombre del DataFrame.

    Retorna:
    - df_sinanidados: DataFrame principal
    - df_desanidado: DataFrame secundario (o None)
    """

    # 🔹 Inicialización por defecto (evita variables no definidas)
    df_sinanidados = df
    df_desanidado = None

    if nombre == "deals" and "custom_fields" in df.columns:
        logger.info("[deals] Procesando columna custom_fields")

        # Tabla principal sin custom_fields
        df_sinanidados = df.drop(columns=["custom_fields"])
        guardar_parquet(logger, df_sinanidados, nombre)

        # Tabla secundaria: deals_custom
        df_base = df[["id", "custom_fields"]]
        desanidado = desanidar_columna(df_base, "custom_fields")

        df_desanidado = pd.concat(
            [df[["id"]], desanidado],
            axis=1
        )

        # Eliminar filas completamente nulas (excepto id)
        df_desanidado = df_desanidado[
            ~(df_desanidado.drop(columns=["id"]).isna()).all(axis=1)
        ]

        logger.info(
            f"[deals] Filas generadas en deals_custom: {len(df_desanidado)}"
        )

    return df_sinanidados, df_desanidado


def expand_stage_durations(
    df: pd.DataFrame,
    id_col: str = "id",
    stages_col: str = "stages_duration",
) -> pd.DataFrame:
    
    registros = []

    for _, row in df[[id_col, stages_col]].iterrows():
        id_value = row[id_col]
        raw_stages = row[stages_col]

        # Saltar si viene vacío
        if pd.isna(raw_stages):
            continue

        # Convertir string → lista si es necesario
        if isinstance(raw_stages, str):
            try:
                lista_stages = ast.literal_eval(raw_stages)
            except Exception:
                continue
        else:
            lista_stages = raw_stages

        # Validar que sea lista
        if not isinstance(lista_stages, list):
            continue

        # Expandir cada diccionario de la lista
        for item in lista_stages:
            if not isinstance(item, dict):
                continue

            dur = item.get("stage_duration", {})

            days = dur.get("days") or 0
            hours = dur.get("hours") or 0
            minutes = dur.get("minutes") or 0

            registros.append({
                "id": id_value,
                "stage_name": item.get("stage_name"),
                "stage_position": item.get("stage_position"),
                "days": days,
                "hours": hours,
                "minutes": minutes,
                "total_days": round(
                    float(days or 0)
                    + float(hours or 0) / 24
                    + float(minutes or 0) / 1440,
                    2
                ),
            })


    return pd.DataFrame(registros)


def obtener_path_parquet(nombre: str) -> Path:
    """
    Retorna la ruta del parquet en data/stage
    """

    root = Path(__file__).resolve().parents[2]  # sube hasta etl_clientify

    path = root / "data" / "stage" / f"{nombre}.parquet"

    return path



def upsert_parquet_por_id_stage(logger, df_new, nombre, id_col="id", stage_col="stage_name"):
    """
    Upsert incremental a parquet histórico usando id + stage_name como clave.
    Reemplaza los registros que ya existen y agrega los nuevos.
    """
    path = obtener_path_parquet(nombre)

    # Asegurarse que los IDs y stages sean strings
    df_new[id_col] = df_new[id_col].astype(str)
    df_new[stage_col] = df_new[stage_col].astype(str)

    if path.exists():
        logger.info("📦 Leyendo parquet histórico")
        df_old = pd.read_parquet(path)

        # Convertir también los IDs y stages antiguos a string
        df_old[id_col] = df_old[id_col].astype(str)
        df_old[stage_col] = df_old[stage_col].astype(str)

        logger.info(f"📊 Registros en histórico: {len(df_old)}")

        # Construir una máscara para eliminar solo los registros que tienen la misma combinación de id+stage
        mask = df_old.apply(lambda row: ((row[id_col], row[stage_col]) 
                                         in zip(df_new[id_col], df_new[stage_col])), axis=1)

        df_old_to_keep = df_old[~mask]

        logger.info(f"♻️ Reemplazando registros existentes por id+stage")
        logger.info(f"📊 Registros a mantener: {len(df_old_to_keep)}")

        # Concatenar los registros antiguos que se mantienen con los nuevos
        df_final = pd.concat([df_old_to_keep, df_new], ignore_index=True)
        logger.info(f"📊 Registros finales después de upsert: {len(df_final)}")
    else:
        logger.info("🆕 No existe parquet — creando nuevo")
        df_final = df_new

    guardar_parquet(logger, df_final, nombre)

def limpiezas_especificas(logger, df: pd.DataFrame, nombre: str) -> pd.DataFrame:
    """Aplica limpiezas específicas según el name de DataFrame."""

    # Eliminar columnas según el diccionario
    if nombre in COLUMNAS_ELIMINAR:
        columnas_a_eliminar = [
            col for col in COLUMNAS_ELIMINAR[nombre] if col in df.columns
        ]
        df = df.drop(columns=columnas_a_eliminar)
        logger.info(f"🗑 Columnas eliminadas: {columnas_a_eliminar}")
    # Otras limpiezas (si aplican)
    if nombre in COLUMNAS_NUMERICAS:
        df = limpiar_columna_numeros(df, COLUMNAS_NUMERICAS[nombre])
        logger.info(f"🔢 Columnas numéricas limpiadas: {COLUMNAS_NUMERICAS[nombre]}")
    if nombre in COLUMNAS_FECHA_HORA:
        df = separar_fecha_hora(df, COLUMNAS_FECHA_HORA[nombre])
        logger.info(f"📅 Columnas fecha/hora separadas: {COLUMNAS_FECHA_HORA[nombre]}")
    return df


def guardar_parquet(logger, df: pd.DataFrame, nombre: str) -> Path:
    """Guarda el DataFrame en formato Parquet y devuelve la ruta."""
    path = root / "data" / "stage"
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    ruta_salida = path / f"{nombre}.parquet"
    df.to_parquet(ruta_salida, index=False, engine="pyarrow")
    logger.info(f"✅ Guardado: {ruta_salida}")
    return ruta_salida


# =============================
# FUNCIÓN DE LIMPIEZA GENERAL
# =============================


def ejecutar_limpieza(logger, df: pd.DataFrame, name: str) -> pd.DataFrame:
    """Ejecuta la limpieza del DataFrame según el name."""
    logger.info(f"🔄 Iniciando limpieza para: {name}")
    df = limpiezas_especificas(logger, df, name)
    # df = df.dropna(how='all')  # Eliminar filas completamente nulas
    return df


def limpieza_anidados(logger, df: pd.DataFrame) -> pd.DataFrame:
    """Ejecuta la limpieza del DataFrame según el name."""
    eliminar_columnas_nulas = lambda df: df.drop(
        columns=[col for col in df.columns if df[col].isna().mean() > 0.01]
    )
    df = eliminar_columnas_nulas(df)
    logger.info(f"🗑 Columnas con más del 1% de nulos eliminadas.")
    return df


def limpiar_archivos(logger):
    """
    Función principal para cargar y limpiar archivos CSV en una carpeta dada.
    """

    ruta = Path(root / "data" / "raw")
    archivos = list(ruta.glob("*.csv"))

    if not archivos:
        logger.info("⚠ No se encontraron archivos CSV en la carpeta.")
        return

    for archivo in archivos:
        nombre = archivo.stem.lower()

        logger.info(f"\n Procesando: {archivo.name} ")
        df = load_data(archivo)
        if nombre == "deals":
            logger.info("Aplicando limpieza específica para deals")
            # df = ejecutar_limpieza(df, nombre)
            df = eliminar_urls(df, nombre)
            # df = limpieza_anidados(df)
            df, df_desanidado = custom_columns(logger, df, nombre)
            guardar_parquet(logger, df, nombre)
            # df_desanidado = limpieza_anidados(df_desanidado)
            guardar_parquet(logger, df_desanidado, f"{nombre}_desanidado")
        elif nombre == "calls":
            logger.info("Aplicando limpieza específica para calls")
            df = ejecutar_limpieza(logger, df, nombre)
            df = eliminar_urls(df, nombre)
            # df = limpieza_anidados(df) # Elimina comentarios
            guardar_parquet(logger, df, nombre)
        elif nombre == "deal_times":

            logger.info("Aplicando limpieza específica para deal_times")

            # Expande los stages y deduplica por id + stage_name
            df_new = expand_stage_durations(
                df,
                id_col="id",
                stages_col="stages_duration"
            )

            # Actualiza el parquet incrementalmente
            upsert_parquet_por_id_stage(logger, df_new, nombre, id_col="id", stage_col="stage_name")


        elif nombre == "users":
            logger.info("Aplicando limpieza específica para users")
            guardar_parquet(logger, df, nombre)
        else:
            df = ejecutar_limpieza(logger, df, nombre)
            df = eliminar_urls(df, nombre)
            # df = limpieza_anidados(df)
            guardar_parquet(logger, df, nombre)
