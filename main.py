from src.extract.clientify_api import *
from src.extract.extract import *
from src.extract.transform import transform_dataset
from src.extract.load import load_to_csv
from src.transform.utils import *
from src.load.load import ejecucion_carga
from test.test_api import *
import sys


def run_extract(logger, full_load: bool = True):
    logger.info("🚀 Iniciando pipeline ETL ")

    for name, df in extract_stream(logger, full_load):

        logger.info(f"\n🔄 Transformando {name}...")

        try:
            df_transformed = transform_dataset(df, name, logger)

            if df_transformed is None:
                logger.info(f"⚠️ transform_dataset devolvió None para {name}, se omite.")
                continue

            # Validar vacío si es DataFrame
            try:
                if getattr(df_transformed, "empty", False):
                    logger.info(f"⚠️ {name} quedó vacío tras transformación, se omite.")
                    continue
            except Exception:
                pass

            # Guardar
            load_to_csv(logger, df_transformed, name, full_load=full_load)

            logger.info(f"✅ {name} procesado y guardado.")

        except Exception as e:
            logger.info(f"❌ Error procesando {name}: {e}")
            enviar_alerta(f"Error procesando {name}: {e}", works=False)


def run_extract_times(logger):
    """
    Función específica para extraer y guardar los tiempos de los deals.
    """
    logger.info(f"\n🔄 Transformando deal_times...")
    try:
        df_times = extraccion_tiempos(logger)
        df_times_transformed = transform_dataset(df_times, "deal_times",logger)
        load_to_csv(logger, df_times_transformed, "deal_times")
    
    except Exception as e:
        logger.info(f"❌ Error procesando deal_times: {e}")
        enviar_alerta(f"Error procesando deal_times: {e}", works=False)
    logger.info(f"✅ deal_times procesado y guardado.")


def run_transform(logger):
    """
    Función principal para transofmar los archivos del antes de cargarlo  al csv
    """
    logger.info(f"\n 🛠️Iniciando transformaciones en: data/raw")
    try:
        limpiar_archivos(logger)
        logger.info("\nLimpieza completada.")
    except Exception as e:
        logger.info(f"❌ Error en run_transform: {e}")
        enviar_alerta(f"Error en transformación (run_transform): {e}", works=False)

def run_load(logger):
    logger.info("\n📤 Iniciando carga de datos a la base de datos SQL...")
    try:
        ejecucion_carga(logger)
        logger.info("\nCarga completada.")
        enviar_alerta(f"ETL ejecutado con éxito", works=True)
    except Exception as e:
        logger.info(f"❌ Error en run_load: {e}")
        enviar_alerta(f"Error en carga SQL (run_load): {e}", works=False)
        

# =============================
# EJECUCIÓN
# =============================
if __name__ == "__main__":
    modo = sys.argv[1] if len(sys.argv) > 1 else "full"

    logger = config_logger()
    if test_api(logger):
        logger.info("🚀 start execution...")
        if modo == "1":
            run_extract(logger, full_load=False)
            run_transform(logger)
            run_load(logger)
            

        elif modo == "2":

            run_extract(logger, full_load=False)
            run_extract_times(logger)
            run_transform(logger)
            run_load(logger)
            

        elif modo == "3":
            run_extract_times(logger)
            run_transform(logger)
            run_load(logger)
            
    else:
        logger.info("⛔ Execution stopped due to API failure.")
        enviar_alerta("ETL failed due to API failure", works=False)
    logger.info("ETL finished.")
