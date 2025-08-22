import os, json, glob
from typing import Dict, List, Tuple
from egon_validation.rules.registry import list_registered
from egon_validation.runner.coverage_analysis import calculate_coverage_stats

def collect(ctx) -> Dict:
    base = os.path.join(ctx.out_dir, ctx.run_id, "tasks")
    items: List[Dict] = []
    datasets_set = set()
    if os.path.isdir(base):
        for path in glob.glob(os.path.join(base, "*", "results.jsonl")):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        obj = json.loads(line)
                        items.append(obj)
                        datasets_set.add(obj.get("dataset"))
                    except Exception:
                        pass
    return {"items": items, "datasets": sorted(d for d in datasets_set if d)}

def _build_formal_rules_index() -> List[str]:
    # Determine all formal rule_ids from registry
    reg = list_registered()
    return sorted({r["rule_id"] for r in reg if r.get("kind") == "formal"})

def _build_custom_checks_map(items: List[Dict]) -> Dict[str, List[str]]:
    # dataset -> list of custom rule names
    reg = list_registered()
    tag_kinds = {"custom", "sanity"}
    tag_ids = {r["rule_id"] for r in reg if r.get("kind") in tag_kinds}
    m: Dict[str, List[str]] = {}
    for it in items:
        if it.get("rule_id") in tag_ids:
            ds = it.get("dataset")
            if not ds: 
                continue
            m.setdefault(ds, [])
            name = it.get("rule_id")
            if name not in m[ds]:
                m[ds].append(name)
    # sort rule names for stable output
    for ds in m:
        m[ds].sort()
    return m

def build_coverage(ctx, collected: Dict) -> Dict:
    items = collected.get("items", [])
    datasets = collected.get("datasets", [])

    # All formal rules from registry (stable column set)
    rules_formal = _build_formal_rules_index()

    # default status/title for every pair
    status = {}      # (dataset, rule_id) -> "na" | "ok" | "fail"
    titles = {}      # (dataset, rule_id) -> tooltip text
    for ds in datasets:
        for rid in rules_formal:
            status[(ds, rid)] = "na"
            titles[(ds, rid)] = "Not applied"

    # apply results
    for it in items:
        rid = it.get("rule_id")
        ds = it.get("dataset")
        if ds and rid in rules_formal:
            ok = bool(it.get("success", False))
            msg = it.get("message") or ""
            key = (ds, rid)
            # if multiple results for same pair exist: any fail dominates
            if not ok:
                status[key] = "fail"
                titles[key] = msg or "Applied: failed"
            else:
                # only set OK if we don't already have a fail
                if status.get(key) != "fail":
                    status[key] = "ok"
                    titles[key] = "Applied: passed"

    cells = [{
        "dataset": ds,
        "rule_id": rid,
        "status": status[(ds, rid)],
        "title": titles[(ds, rid)]
    } for ds in datasets for rid in rules_formal]

    custom_checks = _build_custom_checks_map(items)

    # Calculate comprehensive coverage statistics
    coverage_stats = calculate_coverage_stats(collected, ctx)

    cov = {
        "tables_total": coverage_stats["table_coverage"]["total_tables"],
        "tables_validated": len(datasets),
        "datasets": datasets,
        "rules_formal": rules_formal,
        "cells": cells,
        "custom_checks": custom_checks,
        "coverage_statistics": coverage_stats
    }
    return cov


def write_outputs(ctx, results: Dict, coverage: Dict) -> str:
    out_dir = os.path.join(ctx.out_dir, ctx.run_id, "final")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "results.json"), "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "coverage.json"), "w", encoding="utf-8") as f:
        json.dump(coverage, f, ensure_ascii=False, indent=2)
    return out_dir
