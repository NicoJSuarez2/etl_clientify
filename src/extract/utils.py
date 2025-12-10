from pathlib import Path
import json

CHECKPOINT_FILE = Path("config/checkpoints.json")


def read_checkpoints():
    """Lee el archivo de checkpoints si existe."""
    if CHECKPOINT_FILE.exists():
        return json.loads(CHECKPOINT_FILE.read_text())
    return {}


def write_checkpoints(data: dict):
    """Escribe el diccionario completo de checkpoints en disco."""
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_FILE.write_text(json.dumps(data, indent=2))
