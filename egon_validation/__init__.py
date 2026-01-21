"""Data validation framework for eGon pipeline with PostgreSQL support and
HTML reporting."""

import importlib
import inspect
import pkgutil
from pathlib import Path

__version__ = "1.1.1"

# Core components (exported as public API)
from egon_validation.context import RunContext, RunContextFactory  # noqa: F401
from egon_validation.rules.base import (  # noqa: F401
    Rule,
    SqlRule,
    DataFrameRule,
    RuleResult,
    Severity,
)
from egon_validation.runner.execute import (  # noqa: F401
    run_validations,
    run_for_task,
)


# Auto-discover and import all validation rules
def _load_rules():
    """Automatically discover and import all rule classes from formal and custom packages."""
    rules_dir = Path(__file__).parent / "rules"
    rule_classes = {}

    # Scan both formal and custom rule packages
    for package_name in ["formal", "custom"]:
        package_path = rules_dir / package_name
        if not package_path.exists():
            continue

        # Get all Python modules in the package
        package_module = f"egon_validation.rules.{package_name}"

        try:
            importlib.import_module(package_module)
        except ImportError:
            continue

        # Iterate through all modules in the package
        for _, module_name, _ in pkgutil.iter_modules([str(package_path)]):
            if module_name.startswith("_"):
                continue

            try:
                # Import the module
                full_module_name = f"{package_module}.{module_name}"
                module = importlib.import_module(full_module_name)

                # Find all classes that inherit from Rule
                for name, obj in inspect.getmembers(module, inspect.isclass):
                    if (
                        issubclass(obj, (Rule, SqlRule, DataFrameRule))
                        and obj not in (Rule, SqlRule, DataFrameRule)
                        and obj.__module__ == full_module_name
                    ):
                        rule_classes[name] = obj

            except Exception:
                # Skip modules that fail to import
                continue

    return rule_classes


# Load all rules automatically
_discovered_rules = _load_rules()

# Add discovered rules to global namespace
globals().update(_discovered_rules)

# Build __all__ dynamically
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
] + list(
    _discovered_rules.keys()
)  # Add all discovered rule classes
