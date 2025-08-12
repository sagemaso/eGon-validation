import os
from typing import Optional
from pathlib import Path

def load_env_file():
    """Load environment variables from .env file if it exists"""
    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    if key not in os.environ:  # Don't override existing env vars
                        os.environ[key] = value

def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.environ.get(name, default)

def build_db_url() -> Optional[str]:
    """Build database URL from environment variables"""
    host = get_env("DB_HOST")
    port = get_env("DB_PORT")
    name = get_env("DB_NAME")
    user = get_env("DB_USER")
    password = get_env("DB_PASSWORD")
    
    if all([host, port, name, user, password]):
        return f"postgresql://{user}:{password}@{host}:{port}/{name}"
    return None

# Load .env file on import
load_env_file()

DEFAULT_OUT_DIR = "./validation_runs"
ENV_DB_URL = "EGON_DB_URL"
