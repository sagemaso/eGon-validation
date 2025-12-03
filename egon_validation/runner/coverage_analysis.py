"""
Coverage analysis for validation monitoring
"""

import os
import json
from typing import Dict
from egon_validation.db import make_engine, fetch_one
from egon_validation.config import get_env, ENV_DB_URL, build_db_url
from egon_validation.rules.registry import list_registered
from egon_validation.logging_config import get_logger

logger = get_logger("coverage_analysis")


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

    # Get expected rules from pipeline execution or fallback to registry
    expected_rules_data = collected_data.get("expected_rules", {})

    if expected_rules_data:
        # NEW APPROACH: Count expected rules from what was submitted to pipeline
        logger.debug(f"Using expected rules from {len(expected_rules_data)} tasks")
        expected_rule_ids = set()
        for task_name, rules in expected_rules_data.items():
            for rule in rules:
                expected_rule_ids.add(rule["rule_id"])
        total_rules = len(expected_rule_ids)
        logger.info(f"Rule coverage based on expected pipeline rules: {total_rules} total rules")
    else:
        # Fallback for backward compatibility with standalone mode
        logger.debug("No expected rules found, falling back to registry")
        all_rules = list_registered()
        unique_rule_ids = set(rule["rule_id"] for rule in all_rules)
        total_rules = len(unique_rule_ids)
        logger.info(f"Rule coverage based on registry: {total_rules} registered rules")

    # Count unique applied rules
    applied_rules = set()
    rule_application_count = {}
    successful_applications = 0
    failed_applications = 0

    for item in items:
        rule_id = item.get("rule_id")
        if rule_id:
            applied_rules.add(rule_id)
            rule_application_count[rule_id] = rule_application_count.get(rule_id, 0) + 1

            if item.get("success", False):
                successful_applications += 1
            else:
                failed_applications += 1

    applied_rules_count = len(applied_rules)
    rule_coverage_percent = (
        (applied_rules_count / total_rules * 100) if total_rules > 0 else 0
    )

    total_applications = successful_applications + failed_applications
    success_rate = (
        (successful_applications / total_applications * 100)
        if total_applications > 0
        else 0
    )

    # Rule application statistics
    rule_stats = []
    for rule_id in sorted(applied_rules):
        rule_stats.append(
            {"rule_id": rule_id, "applications": rule_application_count[rule_id]}
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
