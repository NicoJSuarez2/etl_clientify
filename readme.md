# ETL Clientify – Documentación del Proyecto

## Descripción
- Objetivo: Extraer datos desde la API de Clientify, normalizarlos y dejarlos listos para análisis en formato Parquet.
- Salidas: CSV intermedios en data/raw y archivos Parquet en data/stage.
- Registro: Logs detallados en log/etl.log.

## Requisitos
- Python 3.10+ (recomendado entorno virtual)
- Dependencias en requirements.txt
- Archivo .env con credenciales y base URL de la API:

```
TOKEN_CLIENTIFY=TU_TOKEN
BASE_URL=https://api.clientify.net/v1
```

## Estructura de carpetas
- data/raw: CSV crudos resultantes de la extracción.
- data/stage: Parquet listos para consumo analítico.
- log: Archivo de log etl.log con la traza de ejecución.
- src/extract: Extracción + transformaciones ligeras y guardado a CSV.
- src/transform: Limpieza/normalización y guardado a Parquet.

## Cómo se ejecuta (archivos .bat)
Hay dos lanzadores en Windows que activan el entorno y ejecutan el flujo:
- run_etl_semanal.bat → ejecuta python -m main 1
	- Extrae datos (incremental) y transforma a Parquet.
- run_etl.bat → ejecuta python -m main 2
	- Extrae datos (incremental), además genera deal_times y transforma a Parquet.

Importante: Ambos .bat hacen `cd` a una ruta fija (C:\Users\sistemas\Documents\elts\etl_clientify). Si tu proyecto está en otra ruta (por ejemplo C:\Users\bi\Documents\etl_clientify), actualiza esa línea en los .bat o elimínala si los ejecutas desde la carpeta del proyecto.

Alternativa sin .bat (desde la raíz del repo):

```bat
call .venv\Scripts\activate.bat
python -m pip install -r requirements.txt
python -m main 1   :: extracción + transformación
python -m main 2   :: extracción + deal_times + transformación
python -m main 3   :: solo transformación (usa CSV existentes en data/raw)
```

## Modos de ejecución (main.py)
- 1: `run_extract(full_load=False)` y luego `run_transform()`.
- 2: Igual que 1, más `run_extract_times()` para `deal_times`.
- 3: Solo `run_transform()` sobre los CSV ya presentes en data/raw.

Notas:
- El logger se configura vía src/transform/utils.py y guarda en log/etl.log.
- Si ejecutas sin argumento, actualmente no corre ningún modo útil.

## Flujo ETL

### 1) Extract (src/extract)
- Orquestación: [main.py](main.py) → `run_extract()` llama a `extract_all()`.
- Configuración/Endpoints: [src/extract/clientify_api.py](src/extract/clientify_api.py) → `config()` carga .env y define endpoints:
	- contacts, companies, deals, calls, tasks, users, pipelines_stages, tasks/types, deals_pipelines.
- Extracción paginada: `fetch_data()` pagina con `page` y `page_size`; agrega resultados y crea un DataFrame.
- Incrementalidad: se usa el archivo [ultima_fecha.txt](ultima_fecha.txt) para registrar la última ejecución (campo `created[gte]` en las consultas). Durante la extracción (cuando no es carga “full”), se actualiza con la fecha actual UTC (minutos de precisión) al finalizar.
- Extracción general: [src/extract/extract.py](src/extract/extract.py) → `extract_all()` recorre endpoints, llama a `fetch_data()` y devuelve `dict{nombre: DataFrame}`.
- Transformación ligera: [src/extract/transform.py](src/extract/transform.py) → `transform_dataset()` aplica limpieza básica por dataset (normalización de columnas, fechas, drops simples, casos como `deals` y `contacts`).
- Persistencia a CSV: [src/extract/load.py](src/extract/load.py) → `load_to_csv()` escribe cada dataset en data/raw como `<name>.csv` (UTF-8 BOM). 
- Tiempos de deals: `run_extract_times()` usa `extraccion_tiempos()` que lee IDs desde [data/raw/deals.csv](data/raw/deals.csv), consulta cada deal individual y arma el dataset `deal_times` (se guarda también en CSV crudo).

Resultado de Extract:
- Archivos CSV en data/raw: contacts.csv, companies.csv, deals.csv, calls.csv, tasks.csv, users.csv, pipelines_stages.csv, deals_pipelines.csv, y opcionalmente deal_times.csv.

### 2) Transform (src/transform)
- Orquestación: `run_transform()` → `limpiar_archivos()` procesa todos los CSV en data/raw.
- Limpieza y normalización: [src/transform/utils.py](src/transform/utils.py)
	- `COLUMNAS_ELIMINAR`, `COLUMNAS_NUMERICAS`, `COLUMNAS_FECHA_HORA` guían reglas por dataset.
	- `ejecutar_limpieza()` aplica limpieza específica (fechas, numéricos, drops de columnas irrelevantes).
	- `eliminar_urls()` limpia columnas que contienen URLs, conservando IDs relevantes (no aplica a users).
	- `custom_columns()` desanida `custom_fields` de `deals` y genera un archivo adicional `deals_desanidado`.
	- `expand_stage_durations()` expande `stages_duration` para `deal_times`.
	- `guardar_parquet()` guarda la salida en data/stage como `<name>.parquet` (motor pyarrow).

Resultado de Transform:
- Parquets en data/stage por cada CSV de data/raw. Casos especiales:
	- deals.parquet y deals_desanidado.parquet (custom_fields desanidados)
	- deal_times.parquet (duraciones por etapa expandidas)

## Relación entre scripts y módulos
- Orquestación: [main.py](main.py)
- Extracción: [src/extract/extract.py](src/extract/extract.py), [src/extract/clientify_api.py](src/extract/clientify_api.py)
- Transformaciones ligeras (pre-CSV): [src/extract/transform.py](src/extract/transform.py)
- Guardado CSV: [src/extract/load.py](src/extract/load.py)
- Limpieza profunda y Parquet: [src/transform/utils.py](src/transform/utils.py)
- Logs y configuración de logger: [src/transform/utils.py](src/transform/utils.py)

## Consejos y resolución de problemas
- Verifica que .env exista y tenga `TOKEN_CLIENTIFY` válido.
- Si cambias la ubicación del proyecto, actualiza el `cd` de los .bat o ejecútalos desde la carpeta raíz del repo.
- Si `data/raw/deals.csv` no existe, `deal_times` no podrá generarse (no habrá IDs de deals).
- Revisa log/etl.log para entender fallos de red, credenciales o estructura de datos.

## Desarrollo rápido
Para probar solo la transformación con CSV ya generados en data/raw:

```bat
call .venv\Scripts\activate.bat
python -m main 3
```

