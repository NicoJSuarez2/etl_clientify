# ...existing code...
import pandas as pd
import ast


def normalize_dates(df: pd.DataFrame, date_columns: list = None) -> pd.DataFrame:
    """
    Convierte columnas de fechas a formato datetime.
    Args:
        df (pd.DataFrame): dataframe con los datos
        date_columns (list): columnas de fechas (si None, detecta automáticamente)
    Returns:
        pd.DataFrame
    """
    if df.empty:
        return df

    if date_columns is None:
        # Detecta columnas con 'date' o 'created' o 'updated'
        date_columns = [
            col
            for col in df.columns
            if "date" in col.lower() or col.lower() in ["created", "updated"]
        ]

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia nombres de columnas (snake_case) y elimina espacios.
    """
    if df.empty:
        return df
    df.columns = (
        df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("-", "_")
    )
    return df


def eliminar_url(df: pd.DataFrame, col: str = "url") -> pd.DataFrame:
    """
    Elimina la columna de URL si existe.
    """
    if df.empty or col not in df.columns:
        return df
    return df.drop(columns=[col])


def drop_empty_columns(df: pd.DataFrame, threshold: float = 0.9) -> pd.DataFrame:
    """
    Elimina columnas con demasiados valores nulos.
    Args:
        threshold (float): proporción mínima de nulos para eliminar (ej. 0.9 = 90%)
    """
    if df.empty:
        return df
    return df.dropna(axis=1, thresh=int(len(df) * (1 - threshold)))


def expand_stage_durations(
    df: pd.DataFrame, id_col: str = "id", stages_col: str = "stages_duration"
):
    registros = []

    for _, row in df[[id_col, stages_col]].iterrows():
        id_value = row[id_col]

        # Convertir string -> lista de diccionarios
        try:
            lista_stages = ast.literal_eval(row[stages_col])
        except Exception:
            continue

        # Recorrer cada diccionario dentro de la lista
        for item in lista_stages:
            dur = item.get("stage_duration", {})
            registros.append(
                {
                    "id": id_value,
                    "stage_name": item.get("stage_name", None),
                    "days": dur.get("days", None),
                    "hours": dur.get("hours", None),
                    "minutes": dur.get("minutes", None),
                }
            )

    return pd.DataFrame(registros)


def transform_dataset(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """
    Aplica transformaciones específicas según el dataset.
    """
    if df.empty:
        return df

    # Limpieza básica
    df = clean_columns(df)
    df = normalize_dates(df)
    df = eliminar_url(df, col="url")

    # Reglas específicas por dataset
    if dataset_name == "deals":
        if "amount" in df.columns:
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    if dataset_name == "contacts":
        if "email" in df.columns:
            df["email"] = df["email"].str.lower().str.strip()

    if dataset_name == "deals_times":
        df = expand_stage_durations(df, id_col="id", stages_col="stages_duration")

    return df
