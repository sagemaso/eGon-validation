"""
Coverage analysis for validation monitoring
"""

import os
import json
import inspect
import importlib
import pkgutil
from pathlib import Path
from typing import Dict, Set
from egon_validation.db import make_engine, fetch_one
from egon_validation.config import get_env, ENV_DB_URL, build_db_url
from egon_validation.rules.registry import list_registered
from egon_validation.rules.base import Rule
from egon_validation.logging_config import get_logger

logger = get_logger("coverage_analysis")


def discover_all_rule_classes() -> Set[str]:
    """
    Discover all rule classes in the codebase by introspecting rules/formal and rules/custom.

    Returns:
    --------
    Set[str]: Set of all rule class names (e.g., 'RowCountValidation', 'ArrayCardinalityValidation')
    """
    rule_classes = set()

    try:
        # Import rules modules to ensure all rules are loaded
        import egon_validation.rules.formal
        import egon_validation.rules.custom

        # Discover rule classes in formal and custom modules
        for module_name in ['egon_validation.rules.formal', 'egon_validation.rules.custom']:
            try:
                module = importlib.import_module(module_name)
                module_path = Path(module.__file__).parent

                # Iterate through all Python files in the module
                for submodule_info in pkgutil.iter_modules([str(module_path)]):
                    if submodule_info.ispkg or submodule_info.name.startswith('_'):
                        continue

                    # Import the submodule
                    submodule = importlib.import_module(f"{module_name}.{submodule_info.name}")

                    # Find all Rule subclasses in the module
                    for name, obj in inspect.getmembers(submodule, inspect.isclass):
                        # Check if it's a Rule subclass and not the base Rule class itself
                        if issubclass(obj, Rule) and obj is not Rule and obj.__module__ == submodule.__name__:
                            rule_classes.add(obj.__name__)
                            logger.debug(f"Discovered rule class: {obj.__name__} in {submodule.__name__}")

            except Exception as e:
                logger.warning(f"Failed to discover rules in {module_name}: {e}")

        logger.info(f"Discovered {len(rule_classes)} total rule classes: {sorted(rule_classes)}")

    except Exception as e:
        logger.error(f"Failed to discover rule classes: {e}", exc_info=True)

    return rule_classes


def discover_total_tables() -> int:
    """
    Discover total number of tables in the database (excluding system schemas)

    Returns:
    --------
    int: Total number of tables, 0 if database is unavailable
    """
    try:
        logger.info("Attempting to discover total tables in database")
        db_url = get_env(ENV_DB_URL) or build_db_url()
        if not db_url:
            logger.warning("No database URL available - cannot count tables")
            return 0

        logger.debug(f"Connecting to database to count tables")
        engine = make_engine(db_url)
        query = """
        SELECT COUNT(*) as total_tables
        FROM pg_tables
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        """
        result = fetch_one(engine, query)
        engine.dispose()

        total_tables = result.get("total_tables", 0)
        logger.info(f"Successfully discovered {total_tables} tables in database")
        return total_tables
    except Exception as e:
        logger.error(
            f"Failed to discover total tables from database: {type(e).__name__}: {str(e)}",
            extra={"error_type": type(e).__name__, "error": str(e)}
        )
        return 0


def load_saved_table_count(ctx) -> int:
    """Load previously saved table count from metadata file"""
    try:
        metadata_file = os.path.join(
            ctx.out_dir, ctx.run_id, "tasks", "db_metadata.json"
        )
        if os.path.exists(metadata_file):
            logger.debug(f"Loading saved table count from {metadata_file}")
            with open(metadata_file, "r") as f:
                metadata = json.load(f)
                count = metadata.get("total_tables", 0)
                if count > 0:
                    logger.info(f"Loaded saved table count: {count} tables")
                else:
                    logger.warning(f"Saved metadata file exists but contains 0 tables")
                return count
        else:
            logger.debug(f"No saved table count found at {metadata_file}")
    except Exception as e:
        logger.warning(
            f"Failed to load saved table count: {type(e).__name__}: {str(e)}",
            extra={"error_type": type(e).__name__, "error": str(e)}
        )
    return 0


def calculate_coverage_stats(collected_data: Dict, ctx=None) -> Dict:
    """
    Calculate comprehensive coverage statistics

    Parameters:
    -----------
    collected_data: Dict containing items and datasets from aggregate.collect()

    Returns:
    --------
    Dict with coverage statistics
    """
    logger.info("Calculating coverage statistics")
    items = collected_data.get("items", [])
    validated_datasets = set(collected_data.get("datasets", []))

    # Get total tables - try saved count first, then DB if available
    total_tables = 0
    if ctx:
        logger.debug("Attempting to load saved table count from metadata")
        total_tables = load_saved_table_count(ctx)
    if total_tables == 0:
        logger.debug("No saved table count found, discovering from database")
        total_tables = discover_total_tables()

    validated_tables_count = len(validated_datasets)

    # Calculate table coverage percentage
    table_coverage_percent = (
        (validated_tables_count / total_tables * 100) if total_tables > 0 else 0
    )

    logger.info(
        f"Table coverage: {validated_tables_count}/{total_tables} tables validated ({table_coverage_percent:.1f}%)",
        extra={
            "validated_tables": validated_tables_count,
            "total_tables": total_tables,
            "coverage_percent": round(table_coverage_percent, 1)
        }
    )

    # Discover all available rule classes in the codebase
    all_rule_classes = discover_all_rule_classes()
    logger.info(f"Discovered {len(all_rule_classes)} rule classes in codebase: {sorted(all_rule_classes)}")

    # Count unique applied rules by rule_class (not rule_id)
    applied_rule_classes = set()
    rule_class_application_count = {}
    successful_applications = 0
    failed_applications = 0

    for item in items:
        rule_class = item.get("rule_class")
        if rule_class:
            applied_rule_classes.add(rule_class)
            rule_class_application_count[rule_class] = rule_class_application_count.get(rule_class, 0) + 1

            if item.get("success", False):
                successful_applications += 1
            else:
                failed_applications += 1

    # Total rules = union of discovered rules + applied rules (to include external/pipeline rules)
    total_rule_classes = all_rule_classes.union(applied_rule_classes)
    total_rules = len(total_rule_classes)

    applied_rules_count = len(applied_rule_classes)

    # Log any external rule classes (not discovered in codebase)
    external_rules = applied_rule_classes - all_rule_classes
    if external_rules:
        logger.info(f"Detected {len(external_rules)} external rule classes from pipeline projects: {sorted(external_rules)}")

    logger.info(f"Total available rule classes: {total_rules} (codebase: {len(all_rule_classes)}, applied: {applied_rules_count})")

    rule_coverage_percent = (
        (applied_rules_count / total_rules * 100) if total_rules > 0 else 0
    )

    total_applications = successful_applications + failed_applications
    success_rate = (
        (successful_applications / total_applications * 100)
        if total_applications > 0
        else 0
    )

    # Rule application statistics - grouped by rule_class
    rule_stats = []
    for rule_class in sorted(applied_rule_classes):
        rule_stats.append(
            {"rule_class": rule_class, "applications": rule_class_application_count[rule_class]}
        )

    coverage_stats = {
        "table_coverage": {
            "validated_tables": validated_tables_count,
            "total_tables": total_tables,
            "percentage": round(table_coverage_percent, 1),
        },
        "rule_coverage": {
            "applied_rules": applied_rules_count,
            "total_rules": total_rules,
            "percentage": round(rule_coverage_percent, 1),
        },
        "validation_results": {
            "total_applications": total_applications,
            "successful": successful_applications,
            "failed": failed_applications,
            "success_rate": round(success_rate, 1),
        },
        "rule_application_stats": rule_stats,
    }

    return coverage_stats
