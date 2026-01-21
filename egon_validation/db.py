"""Database connection and query utilities."""

from typing import Any, Dict, List, Optional, Union, Iterable
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DisconnectionError

import pandas as pd
import geopandas as gpd
from egon_validation.retry import database_retry, connection_circuit_breaker
from egon_validation.logging_config import get_logger
from egon_validation.exceptions import DatabaseConnectionError

logger = get_logger("db")

# Database connection pool configuration
# Pool size matches default ThreadPoolExecutor max_workers (6) + 1 for main thread
DEFAULT_POOL_SIZE = 7
# Allow temporary bursts up to 10 total connections (7 + 3)
DEFAULT_MAX_OVERFLOW = 3
# Recycle connections after 30 minutes to prevent stale connections
POOL_RECYCLE_SECONDS = 1800


def make_engine(db_url: str, echo: bool = False) -> Engine:
    """Create SQLAlchemy engine with connection pooling.

    Args:
        db_url: Database connection URL
        echo: If True, log all SQL statements

    Returns:
        Configured SQLAlchemy Engine with connection pooling
    """
    return create_engine(
        db_url,
        echo=echo,
        pool_size=DEFAULT_POOL_SIZE,
        max_overflow=DEFAULT_MAX_OVERFLOW,
        pool_pre_ping=True,  # Verify connection health before using
        pool_recycle=POOL_RECYCLE_SECONDS,
    )


@database_retry
@connection_circuit_breaker
def fetch_one(
    engine: Engine, sql: str, params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Execute SQL and return first row as dict."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text(sql), params or {}).mappings().first()
            result = dict(row or {})
            logger.debug("Successfully fetched one row", extra={"sql": sql[:100]})
            return result
    except (OperationalError, DisconnectionError) as e:
        logger.error(
            f"Database error in fetch_one: {str(e)}",
            extra={"sql": sql[:100], "error": str(e)},
        )
        raise DatabaseConnectionError(f"Failed to fetch data: {str(e)}") from e


@database_retry
def fetch_all(
    engine: Engine, sql: str, params: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """Execute SQL and return all rows as list of dicts."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params or {}).mappings().all()
            result = [dict(r) for r in rows]
            logger.debug(
                f"Successfully fetched {len(result)} rows",
                extra={"sql": sql[:100], "row_count": len(result)},
            )
            return result
    except (OperationalError, DisconnectionError) as e:
        logger.error(
            f"Database error in fetch_all: {str(e)}",
            extra={"sql": sql[:100], "error": str(e)},
        )
        raise DatabaseConnectionError(f"Failed to fetch data: {str(e)}") from e


@database_retry
def fetch_dataframe(
    engine: Engine,
    sql: str,
    params: Optional[Dict[str, Any]] = None,
    chunksize: Optional[int] = None,
) -> Union["pd.DataFrame", Iterable["pd.DataFrame"]]:
    """Execute SQL and return results as pandas DataFrame.

    Args:
        engine: SQLAlchemy engine
        sql: SQL query string
        params: Query parameters
        chunksize: If specified, return iterator of DataFrames

    Returns:
        DataFrame or iterator of DataFrames

    Raises:
        DatabaseConnectionError: On database connection issues
    """
    try:
        df = pd.read_sql_query(text(sql), engine, params=params, chunksize=chunksize)
        if chunksize is None:
            logger.debug(
                f"Successfully fetched DataFrame with {len(df)} rows",
                extra={"sql": sql[:100], "row_count": len(df)},
            )
        else:
            logger.debug(
                "Successfully created DataFrame iterator",
                extra={"sql": sql[:100]},
            )
        return df
    except (OperationalError, DisconnectionError) as e:
        logger.error(
            f"Database error in fetch_dataframe: {str(e)}",
            extra={"sql": sql[:100], "error": str(e)},
        )
        raise DatabaseConnectionError(f"Failed to fetch DataFrame: {str(e)}") from e


def fetch_geodataframe(
    engine: Engine,
    sql: str,
    geom_col: str = "geom",
    params: Optional[Dict[str, Any]] = None,
    crs: Optional[str] = None,
) -> "gpd.GeoDataFrame":
    """Execute SQL and return results as geopandas GeoDataFrame.

    Args:
        engine: SQLAlchemy engine
        sql: SQL query string
        geom_col: Name of geometry column
        params: Query parameters
        crs: Coordinate reference system

    Returns:
        GeoDataFrame

    Raises:
    """

    gdf = gpd.read_postgis(text(sql), engine, params=params, geom_col=geom_col)
    if crs:
        gdf.set_crs(crs, inplace=True)
    return gdf


class DataInterface:
    """Enhanced database interface with DataFrame support."""

    def __init__(self, engine: Engine):
        self.engine = engine

    def fetch_one_dict(
        self, sql: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute SQL and return first row as dict (performance-optimized)."""
        return fetch_one(self.engine, sql, params)

    def fetch_all_dict(
        self, sql: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute SQL and return all rows as list of dicts (performance-optimized)."""
        return fetch_all(self.engine, sql, params)

    def fetch_dataframe(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        chunksize: Optional[int] = None,
    ) -> Union["pd.DataFrame", Iterable["pd.DataFrame"]]:
        """Execute SQL and return DataFrame for analytics."""
        return fetch_dataframe(self.engine, sql, params, chunksize)

    def fetch_geodataframe(
        self,
        sql: str,
        geom_col: str = "geom",
        params: Optional[Dict[str, Any]] = None,
        crs: Optional[str] = None,
    ) -> "gpd.GeoDataFrame":
        """Execute SQL and return GeoDataFrame for PostGIS data."""
        return fetch_geodataframe(self.engine, sql, geom_col, params, crs)
