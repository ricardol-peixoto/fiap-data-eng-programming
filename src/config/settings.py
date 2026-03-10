import yaml
import os
from pathlib import Path

def carregar_config():
    # Detecta a raiz do projeto (ajuste o número de .parent conforme sua pasta) para tornar o projeto agnóstico à ferramenta.
    root_dir = Path(os.getcwd())
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        config_path = root_dir.parent / "config" / "settings.yaml"
    else:
        config_path = root_dir / "config" / "settings.yaml"
        
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    
    return config, root_dir

config, project_root = carregar_config()