import glob
import pandas as pd
from pathlib import Path
from sqlalchemy import inspect, text

# 👉 IMPORTAS TU LOGGER EXISTENTE
from transform.utils import config_logger  # ajusta la ruta si es diferente

logger = config_logger()


def validar_columnas(engine, schema, tabla, df):
    """
    Valida que las columnas del DataFrame coincidan con la tabla en SQL Server
    """
    logger.info(f"[{tabla}] Iniciando validación de columnas")

    inspector = inspect(engine)

    columnas_sql = [
        col["name"].lower()
        for col in inspector.get_columns(tabla, schema=schema)
    ]

    columnas_df = df.columns.str.lower().tolist()

    faltantes = set(columnas_sql) - set(columnas_df)
    extras = set(columnas_df) - set(columnas_sql)

    if faltantes:
        logger.error(
            f"[{tabla}] Columnas faltantes en DataFrame: {faltantes}"
        )
        raise ValueError(f"Columnas faltantes en {tabla}")

    if extras:
        logger.warning(
            f"[{tabla}] Columnas extra en DataFrame (se eliminarán): {extras}"
        )

    logger.info(f"[{tabla}] Validación de columnas exitosa")

    return extras


def truncate_table(engine, schema, tabla):
    """
    Elimina todos los registros de la tabla manteniendo la estructura
    """
    logger.warning(f"[{tabla}] Ejecutando TRUNCATE TABLE")

    with engine.begin() as conn:
        conn.execute(
            text(f"TRUNCATE TABLE {schema}.{tabla}")
        )

    logger.info(f"[{tabla}] TRUNCATE TABLE ejecutado correctamente")


def load_existing_tables(
    engine,
    parquet_path,
    schema="dbo",
    mode="append",      # append | truncate
    chunksize=5000
):
    """
    Carga archivos parquet en tablas SQL Server existentes
    """
    logger.info("=== INICIO PROCESO LOAD SQL SERVER ===")
    logger.info(f"Ruta parquet: {parquet_path}")
    logger.info(f"Modo de carga: {mode}")

    for archivo in glob.glob(parquet_path):
        tabla = Path(archivo).stem
        logger.info(f"[{tabla}] Iniciando carga")

        try:
            # 1️⃣ Lectura del parquet
            logger.info(f"[{tabla}] Leyendo archivo parquet")
            df = pd.read_parquet(archivo)

            # 2️⃣ Normalización de columnas
            logger.info(f"[{tabla}] Normalizando nombres de columnas")
            df.columns = (
                df.columns
                .str.lower()
                .str.replace(" ", "_")
            )

            # 3️⃣ Validación de esquema
            extras = validar_columnas(
                engine=engine,
                schema=schema,
                tabla=tabla,
                df=df
            )

            # 4️⃣ Eliminación de columnas extra
            if extras:
                df = df.drop(columns=extras)
                logger.info(
                    f"[{tabla}] Columnas extra eliminadas"
                )

            # 5️⃣ Truncate si aplica
            if mode == "truncate":
                truncate_table(engine, schema, tabla)

            # 6️⃣ Inserción de datos
            logger.info(
                f"[{tabla}] Insertando {len(df)} registros"
            )

            df.to_sql(
                name=tabla,
                con=engine,
                schema=schema,
                if_exists="append",
                index=False,
                chunksize=chunksize,
                method="multi"
            )

            logger.info(f"[{tabla}] Carga finalizada correctamente")

        except Exception as e:
            logger.exception(
                f"[{tabla}] Error durante el proceso de carga"
            )
            raise e

    logger.info("=== FIN PROCESO LOAD SQL SERVER ===")
