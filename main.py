from src.extract.clientify_api import *
from src.extract.extract import *
from src.extract.transform import transform_dataset
from src.extract.load import load_to_csv
from src.transform.utils import *
from src.load.load import ejecucion_carga
import sys


def run_extract(logger, full_load: bool = True):
    # Extraer todos los datos
    logger.info("Iniciando extracción de datos...")
    try:
        all_data = extract_all(logger, full_load=full_load)
    except Exception as e:
        logger.info(f"❌ Error en extracción: {e}")
        return
    if not all_data:
        logger.info("⚠️ all_data está vacío. No hay datasets para transformar/guardar.")
        return
    # Transformar y guardar
    for name, df in all_data.items():
        logger.info(f"\n🔄 Transformando {name}...")
        try:
            df_transformed = transform_dataset(df, name)
            if df_transformed is None:
                logger.info(f"⚠️ transform_dataset devolvió None para {name}, se omite.")
                continue
            # Si es un DataFrame, opcionalmente comprobar si está vacío
            try:
                is_empty = getattr(df_transformed, "empty", False)
                if is_empty:
                    logger.info(
                        f"⚠️ El DataFrame transformado de {name} está vacío, se omite."
                    )
                    continue
            except Exception:
                pass
            # Guardar en process/
            # load_to_parquet(df_transformed, name)
            load_to_csv(logger, df_transformed, name, full_load=full_load)
            logger.info(f"✅ {name} procesado y guardado.")
        except Exception as e:
            logger.info(f"❌ Error procesando {name}: {e}")


def run_extract_times(logger):
    """
    Función específica para extraer y guardar los tiempos de los deals.
    """
    logger.info(f"\n🔄 Transformando deal_times...")
    #df_times = extraccion_tiempos(logger)
    #transform_dataset(df_times, "deal_times")
    #load_to_csv(logger, df_times, "deal_times")
    logger.info(f"✅ deal_times procesado y guardado.")


def run_transform(logger):
    """
    Función principal para cargar y limpiar archivos CSV en una carpeta dada.
    """
    logger.info(f"\n 🛠️Iniciando transfomraciones en: data/raw")
    limpiar_archivos(logger)
    logger.info("\nLimpieza completada.")

def run_load(logger):
    """
    Función principal para cargar archivos Parquet desde data/stage a la base de datos SQL.
    """
    logger.info(f"\n📤Iniciando carga de datos a la base de datos SQL...")
    ejecucion_carga(logger)
    logger.info("\nCarga completada.")

# =============================
# EJECUCIÓN
# =============================
if __name__ == "__main__":
    modo = sys.argv[1] if len(sys.argv) > 1 else "full"

    logger = config_logger()

    if modo == "1":
        run_extract(logger, full_load=False)
        run_transform(logger)
        run_load(logger)

    elif modo == "2":
        run_extract(logger, full_load=True)
        run_extract_times(logger)
        run_transform(logger)
        run_load(logger)

    elif modo == "3":
        run_transform(logger)
        run_load(logger)

    logger.info("Proceso ETL completado.")
