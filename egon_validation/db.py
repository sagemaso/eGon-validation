from typing import Any, Dict, List, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from egon_validation.retry_utils import retry_database_operation
from egon_validation.logging_config import get_logger

logger = get_logger("db")

def make_engine(db_url: str, echo: bool = False) -> Engine:
    return create_engine(
        db_url, 
        future=True, 
        echo=echo,
        pool_size=20,
        max_overflow=30,
        pool_pre_ping=True,
        pool_recycle=3600
    )

@retry_database_operation
def fetch_one(engine: Engine, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    logger.debug("Executing database query", extra={"sql": sql[:100], "has_params": bool(params)})
    with engine.connect() as conn:
        row = conn.execute(text(sql), params or {}).mappings().first()
        return dict(row or {})

@retry_database_operation
def fetch_all(engine: Engine, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    logger.debug("Executing database query (fetch all)", extra={"sql": sql[:100], "has_params": bool(params)})
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params or {}).mappings().all()
        return [dict(r) for r in rows]
