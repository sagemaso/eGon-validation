import os
import json
import glob
from datetime import datetime
from typing import Dict, List
from egon_validation.rules.registry import list_registered
from egon_validation.runner.coverage_analysis import calculate_coverage_stats


def collect(ctx) -> Dict:
    base = os.path.join(ctx.out_dir, ctx.run_id, "tasks")
    items: List[Dict] = []
    datasets_set = set()
    expected_rules: Dict[str, List[Dict]] = {}

    if os.path.isdir(base):
        # structure: tasks/<task_name>/<rule_id>/results.jsonl
        for path in glob.glob(os.path.join(base, "*", "*", "results.jsonl")):
            with open(path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                # Take only the last line (most recent result)
                if lines:
                    try:
                        obj = json.loads(lines[-1])
                        items.append(obj)
                        table = obj.get("table")
                        datasets_set.add(table)
                    except Exception:
                        pass

        # Collect expected rules for each task
        # structure: tasks/<task_name>/expected_rules.json
        for task_dir_path in glob.glob(os.path.join(base, "*")):
            if os.path.isdir(task_dir_path):
                task_name = os.path.basename(task_dir_path)
                expected_file = os.path.join(task_dir_path, "expected_rules.json")
                if os.path.exists(expected_file):
                    try:
                        with open(expected_file, "r", encoding="utf-8") as f:
                            expected_rules[task_name] = json.load(f)
                    except Exception:
                        pass

    return {
        "items": items,
        "datasets": sorted(d for d in datasets_set if d),
        "expected_rules": expected_rules,
    }


def _build_formal_rules_index(collected_items: List[Dict]) -> List[str]:
    """
    Determine formal rule classes to display in coverage matrix.

    Extracts unique rule_class names from collected validation results.

    Parameters:
    -----------
    collected_items: List of validation result items with rule_class field

    Returns:
    --------
    Sorted list of formal rule class names
    """
    rule_classes = set()
    for item in collected_items:
        if item.get("kind") == "formal" and item.get("rule_class"):
            rule_classes.add(item["rule_class"])
    return sorted(rule_classes)


def _build_custom_checks_map(
    items: List[Dict], expected_rules: Dict[str, List[Dict]] = None
) -> Dict[str, List[str]]:
    """
    Build map of table -> list of custom/sanity rule names.

    If expected_rules is provided, uses those to filter.
    Otherwise falls back to registry.

    Parameters:
    -----------
    items: List of execution result items
    expected_rules: Dict mapping task_name -> list of expected rule dicts

    Returns:
    --------
    Dict mapping table name -> list of custom/sanity rule_ids
    """
    # table -> list of custom rule names
    tag_kinds = {"custom", "sanity"}

    if expected_rules:
        # NEW: Use expected rules from pipeline execution
        tag_ids = set()
        for task_name, rules in expected_rules.items():
            for rule in rules:
                if rule.get("kind") in tag_kinds:
                    tag_ids.add(rule["rule_id"])
    else:
        # Fallback: Use registry
        reg = list_registered()
        tag_ids = {r["rule_id"] for r in reg if r.get("kind") in tag_kinds}

    m: Dict[str, List[str]] = {}
    for it in items:
        if it.get("rule_id") in tag_ids:
            tbl = it.get("table")
            if not tbl:
                continue
            m.setdefault(tbl, [])
            name = it.get("rule_id")
            if name not in m[tbl]:
                m[tbl].append(name)
    # sort rule names for stable output
    for tbl in m:
        m[tbl].sort()
    return m


def build_coverage(ctx, collected: Dict) -> Dict:
    items = collected.get("items", [])
    datasets = collected.get("datasets", [])
    expected_rules = collected.get("expected_rules", {})

    # All formal rule classes - from collected items
    rules_formal = _build_formal_rules_index(items)

    # default status/title for every pair
    status = {}  # (dataset, rule_class) -> "na" | "ok" | "fail"
    titles = {}  # (dataset, rule_class) -> tooltip text
    for ds in datasets:
        for rule_class in rules_formal:
            status[(ds, rule_class)] = "na"
            titles[(ds, rule_class)] = "Not applied"

    # apply results
    for it in items:
        rule_class = it.get("rule_class")
        tbl = it.get("table")
        if tbl and rule_class in rules_formal:
            ok = bool(it.get("success", False))
            msg = it.get("message") or ""
            key = (tbl, rule_class)
            # if multiple results for same pair exist: any fail dominates
            if not ok:
                status[key] = "fail"
                titles[key] = msg or "Applied: failed"
            else:
                # only set OK if we don't already have a fail
                if status.get(key) != "fail":
                    status[key] = "ok"
                    titles[key] = "Applied: passed"

    cells = [
        {
            "dataset": ds,
            "rule_id": rule_class,  # Keep field name for compatibility with JS
            "status": status[(ds, rule_class)],
            "title": titles[(ds, rule_class)],
        }
        for ds in datasets
        for rule_class in rules_formal
    ]

    custom_checks = _build_custom_checks_map(items, expected_rules)

    # Calculate comprehensive coverage statistics
    coverage_stats = calculate_coverage_stats(collected, ctx)

    cov = {
        "tables_total": coverage_stats["table_coverage"]["total_tables"],
        "tables_validated": len(datasets),
        "datasets": datasets,
        "rules_formal": rules_formal,
        "cells": cells,
        "custom_checks": custom_checks,
        "coverage_statistics": coverage_stats,
    }
    return cov


def write_outputs(ctx, results: Dict, coverage: Dict) -> str:
    task_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join(ctx.out_dir, ctx.run_id, f"final.{task_timestamp}")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "results.json"), "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "coverage.json"), "w", encoding="utf-8") as f:
        json.dump(coverage, f, ensure_ascii=False, indent=2)
    return out_dir
