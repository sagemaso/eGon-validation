"""Configuration management with environment variable loading and database URL building."""

import os
from typing import Optional
from pathlib import Path

def load_env_file():
    """Load environment variables from .env file if it exists."""
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
    """Get environment variable with optional default."""
    return os.environ.get(name, default)

def build_db_url() -> Optional[str]:
    """Build PostgreSQL URL from DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD env vars."""
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

# Core Configuration Constants
DEFAULT_OUT_DIR = "./validation_runs"
"""str: Default directory for storing validation results and reports."""

ENV_DB_URL = "EGON_DB_URL"
"""str: Environment variable name for complete database URL override."""

# ElectricalLoadAggregationValidation
ELECTRICAL_LOAD_EXPECTED_VALUES = {
    "eGon2035": {"sum_twh": 533.48, "max_gw": 109.38, "min_gw": 31.60},
    "eGon2021": {"sum_twh": 500.0, "max_gw": 75.0, "min_gw": 25.0},
    "eGon100RE": {"sum_twh": 581.98, "max_gw": 107.44, "min_gw": 40.15},
    "eGon2035_lowflex": {"sum_twh": 533.48, "max_gw": 109.38, "min_gw": 31.60}
}

# ArrayCardinalityValidation
ARRAY_CARDINALITY_ANNUAL_HOURS = 8760

# RangeValidation
LOAD_PROFILE_MIN_VALUE = 0.0
LOAD_PROFILE_MAX_VALUE = 1.2

# RowCountCheck
MV_GRID_DISTRICTS_COUNT = 3854

# SRIDValidation
DEFAULT_SRID = 4326

# Tolerances
BALANCE_CHECK_TOLERANCE = 0.0
DISAGGREGATED_DEMAND_TOLERANCE = 0.01

