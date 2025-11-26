"""Data validation framework for eGon pipeline with PostgreSQL support and
HTML reporting."""

__version__ = "0.1.0"

# Core components
from egon_validation.context import RunContext, RunContextFactory
from egon_validation.rules.base import Rule, SqlRule, DataFrameRule, RuleResult, Severity
from egon_validation.runner.execute import run_validations, run_for_task

# Validation rules (for inline declaration in datasets)
from egon_validation.rules.formal.row_count_check import RowCountValidation
from egon_validation.rules.formal.array_cardinality_check import (
    ArrayCardinalityValidation,
)
from egon_validation.rules.formal.referential_integrity_check import (
    ReferentialIntegrityValidation,
)
from egon_validation.rules.formal.null_check import NotNullAndNotNaN
from egon_validation.rules.custom.row_count_comparison import RowCountComparisonValidation

__all__ = [
    # Version
    "__version__",
    # Core
    "RunContext",
    "RunContextFactory",
    "Rule",
    "SqlRule",
    "DataFrameRule",
    "RuleResult",
    "Severity",
    # Execution
    "run_validations",
    "run_for_task",
    # Validation rules
    "RowCountValidation",
    "RowCountComparisonValidation",
    "ArrayCardinalityValidation",
    "ReferentialIntegrityValidation",
    "NotNullAndNotNaN",
]
