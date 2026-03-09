import yaml
import os
from pathlib import Path

def carregar_config():
    # Detecta a raiz do projeto (ajuste o número de .parent conforme sua pasta)
    # Se o script estiver em /src/main.py, usamos .parent.parent
    root_dir = Path(os.getcwd())
    config_path = root_dir.parent / "config" / "settings.yaml"

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    
    return config, root_dir

# --- Uso Prático ---
config, project_root = carregar_config()