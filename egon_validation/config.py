import os
from typing import Optional

def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.environ.get(name, default)

DEFAULT_OUT_DIR = "./validation_runs"
ENV_DB_URL = "EGON_DB_URL"
