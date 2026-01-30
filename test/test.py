#%%
import glob
import pandas as pd
from pathlib import Path

archivos = []

def cargar_archivos():
    for i in glob.glob("./data/stage/*.parquet"):
        archivos.append(i)
    print(f"Archivos cargados correctamente. {len(archivos)} archivos encontrados.")


def diccionario_datos():
    """
    Genera un diccionario de datos consolidado
    para todos los archivos parquet
    """
    diccionario = []

    for archivo in archivos:
        df = pd.read_parquet(archivo)

        for col in df.columns:
            diccionario.append({
                "archivo": Path(archivo).stem,
                "columna": col,
                "tipo_dato": str(df[col].dtype),
                "registros_totales": len(df),
                "nulos": df[col].isna().sum(),
                "porcentaje_nulos": round(df[col].isna().mean() * 100, 2),
                "ejemplo_valor": df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            })

    dic_df = pd.DataFrame(diccionario)
    return dic_df


cargar_archivos()
dic_df = diccionario_datos()

# Guardar resultado
dic_df.to_excel("doc/diccionario_datos_stage.xlsx", index=False)

print("Diccionario de datos generado correctamente.")

#%%
import pandas as pd
import glob
from pathlib import Path
import os


def eda(df: pd.DataFrame) -> pd.DataFrame:
    resumen = pd.DataFrame({
        "columna": df.columns,
        "tipo_dato": df.dtypes,
        "registros_totales": [len(df)] * len(df.columns),
        "nulos": df.isna().sum(),
        "porcentaje_nulos": round(df.isna().mean() * 100, 2),
        "valores_unicos": df.nunique(),
    })
    return resumen


def generar_archivos(path_origen: str, path_destino: str):
    os.makedirs(path_destino, exist_ok=True)

    archivo_salida = os.path.join(path_destino, "eda_Clientify.xlsx")

    with pd.ExcelWriter(archivo_salida, engine="xlsxwriter") as writer:
        for archivo in glob.glob(os.path.join(path_origen, "*.parquet")):
            df = pd.read_parquet(archivo)
            resumen_df = eda(df)

            nombre_hoja = Path(archivo).stem[:31]  # Excel máx 31 chars
            resumen_df.to_excel(
                writer,
                sheet_name=nombre_hoja,
                index=False
            )

            print(f"EDA agregado a hoja: {nombre_hoja}")

    print(f"\nArchivo final generado: {archivo_salida}")



def ejecucion():
    path_origen = r"C:\Users\bi\Documents\etl_clientify\data\stage"
    path_destino = r"C:\Users\bi\Documents\etl_clientify\data\eda"

    generar_archivos(path_origen, path_destino)


if __name__ == "__main__":
    ejecucion()
