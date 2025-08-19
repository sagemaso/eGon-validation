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

# ElectricalLoadAggregationValidation
ELECTRICAL_LOAD_EXPECTED_VALUES = {
    "eGon2035": {"sum_twh": 533.48, "max_gw": 109.38, "min_gw": 31.60},
    "eGon2021": {"sum_twh": 500.0, "max_gw": 75.0, "min_gw": 25.0},
    "eGon100RE": {"sum_twh": 581.98, "max_gw": 107.44, "min_gw": 40.15},
    "eGon2035_lowflex": {"sum_twh": 533.48, "max_gw": 109.38, "min_gw": 31.60}
}

# ArrayCardinalityValidation
ARRAY_CARDINALITY_ANNUAL_HOURS = 8760
ARRAY_CARDINALITY_ANNUAL_DAYS = 365

# RangeValidation
LOAD_PROFILE_MIN_VALUE = 0.0
LOAD_PROFILE_MAX_VALUE = 1.2

# RowCountCheck
MV_GRID_DISTRICTS_COUNT = 3854

# SRIDValidation
DEFAULT_SRID = 4326
PROJECTED_SRID = 3035

# Geographic bounds
GERMANY_LAT_BOUNDS = [45, 55]
GERMANY_LON_BOUNDS = [5, 15]
EUROPE_LAT_BOUNDS = [35, 70]
EUROPE_LON_BOUNDS = [-10, 30]

# Power system limits
MAX_LOAD_GW = 10.0
HOME_BATTERY_MAX_POWER_KW = 50
HOME_BATTERY_MAX_ENERGY_KWH = 200
HOME_BATTERY_MAX_DURATION_H = 20
ROOFTOP_PV_MAX_CAPACITY_KW = 1000
ROOFTOP_PV_MIN_CAPACITY_KW = 0.001
ROOFTOP_PV_REASONABLE_MAX_KW = 100

# Tolerances
BALANCE_CHECK_TOLERANCE = 0.0
RESIDENTIAL_ELECTRICITY_RTOL = 1e-5
DSM_SANITY_ATOL = 1e-1
DISAGGREGATED_DEMAND_TOLERANCE = 0.01

# E-mobility thresholds
EMOBILITY_MAX_EV_PER_GRID = 1000
EMOBILITY_MAX_EMPTY_GRID_FRACTION = 0.5
EMOBILITY_MAX_CONCENTRATED_GRID_FRACTION = 0.1
