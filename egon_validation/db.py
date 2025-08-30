"""Database connection and query utilities."""

from typing import Any, Dict, List, Optional, Union, Iterable
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

import pandas as pd
import geopandas as gpd

def make_engine(db_url: str, echo: bool = False) -> Engine:
    """Create SQLAlchemy engine with connection pooling."""
    return create_engine(
        db_url, 
        future=True, 
        echo=echo,
        pool_size=20,
        max_overflow=30,
        pool_pre_ping=True,
        pool_recycle=3600
    )

def fetch_one(engine: Engine, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Execute SQL and return first row as dict."""
    with engine.connect() as conn:
        row = conn.execute(text(sql), params or {}).mappings().first()
        return dict(row or {})

def fetch_all(engine: Engine, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Execute SQL and return all rows as list of dicts."""
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params or {}).mappings().all()
        return [dict(r) for r in rows]

def fetch_dataframe(
    engine: Engine, 
    sql: str, 
    params: Optional[Dict[str, Any]] = None,
    chunksize: Optional[int] = None
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
    """
    
    return pd.read_sql_query(text(sql), engine, params=params, chunksize=chunksize)

def fetch_geodataframe(
    engine: Engine,
    sql: str,
    geom_col: str = "geom",
    params: Optional[Dict[str, Any]] = None,
    crs: Optional[str] = None
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

    def fetch_one_dict(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute SQL and return first row as dict (performance-optimized)."""
        return fetch_one(self.engine, sql, params)

    def fetch_all_dict(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute SQL and return all rows as list of dicts (performance-optimized)."""
        return fetch_all(self.engine, sql, params)

    def fetch_dataframe(
        self, 
        sql: str, 
        params: Optional[Dict[str, Any]] = None,
        chunksize: Optional[int] = None
    ) -> Union["pd.DataFrame", Iterable["pd.DataFrame"]]:
        """Execute SQL and return DataFrame for analytics."""
        return fetch_dataframe(self.engine, sql, params, chunksize)

    def fetch_geodataframe(
        self,
        sql: str,
        geom_col: str = "geom",
        params: Optional[Dict[str, Any]] = None,
        crs: Optional[str] = None
    ) -> "gpd.GeoDataFrame":
        """Execute SQL and return GeoDataFrame for PostGIS data."""
        return fetch_geodataframe(self.engine, sql, geom_col, params, crs)
