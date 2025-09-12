"""
Coverage analysis for validation monitoring
"""

import os
from typing import Dict
from egon_validation.db import make_engine, fetch_one
from egon_validation.config import get_env, ENV_DB_URL, build_db_url
from egon_validation.rules.registry import list_registered


def discover_total_tables() -> int:
    """
    Discover total number of tables in the database (excluding system schemas)

    Returns:
    --------
    int: Total number of tables, 0 if database is unavailable
    """
    try:
        db_url = get_env(ENV_DB_URL) or build_db_url()
        if not db_url:
            return 0

        engine = make_engine(db_url)
        query = """
        SELECT COUNT(*) as total_tables
        FROM pg_tables
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        """
        result = fetch_one(engine, query)
        return result.get("total_tables", 0)
    except Exception:
        # Database unavailable (e.g., SSH tunnel closed), fallback to 0
        return 0


def load_saved_table_count(ctx) -> int:
    """Load previously saved table count from metadata file"""
    try:
        import json

        metadata_file = os.path.join(
            ctx.out_dir, ctx.run_id, "tasks", "db_metadata.json"
        )
        if os.path.exists(metadata_file):
            with open(metadata_file, "r") as f:
                metadata = json.load(f)
                return metadata.get("total_tables", 0)
    except Exception:
        pass
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
    items = collected_data.get("items", [])
    validated_datasets = set(collected_data.get("datasets", []))

    # Get total tables - try saved count first, then DB if available
    total_tables = 0
    if ctx:
        total_tables = load_saved_table_count(ctx)
    if total_tables == 0:
        total_tables = discover_total_tables()

    validated_tables_count = len(validated_datasets)

    # Calculate table coverage percentage
    table_coverage_percent = (
        (validated_tables_count / total_tables * 100) if total_tables > 0 else 0
    )

    # Get all registered rules
    all_rules = list_registered()
    total_rules = len(all_rules)

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
