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
