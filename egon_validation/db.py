from typing import Any, Dict, List, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

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

def fetch_one(engine: Engine, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    with engine.connect() as conn:
        row = conn.execute(text(sql), params or {}).mappings().first()
        return dict(row or {})

def fetch_all(engine: Engine, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params or {}).mappings().all()
        return [dict(r) for r in rows]
