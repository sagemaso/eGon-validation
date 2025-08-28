import pytest
from unittest.mock import Mock, MagicMock
from sqlalchemy import create_engine
from egon_validation.context import RunContext


@pytest.fixture
def mock_engine():
    """Mock SQLAlchemy engine for testing."""
    engine = Mock()
    engine.execute = Mock()
    return engine


@pytest.fixture
def mock_context():
    """Mock run context for testing."""
    ctx = Mock(spec=RunContext)
    ctx.run_id = "test_run"
    ctx.out_dir = "test_output"
    ctx.extra = {"scenario": "test_scenario", "task": "test_task"}
    return ctx


@pytest.fixture
def sample_db_row():
    """Sample database row result."""
    return {"count": 5, "n_bad": 0, "total": 100}


@pytest.fixture
def empty_db_row():
    """Empty database row result."""
    return {"count": 0, "n_bad": 0, "total": 0}