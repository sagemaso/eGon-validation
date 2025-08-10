import os, json, glob
from typing import Dict, List, Tuple
from egon_validation.rules.registry import list_registered

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
    custom_ids = {r["rule_id"] for r in reg if r.get("kind") == "custom"}
    m: Dict[str, List[str]] = {}
    for it in items:
        if it.get("rule_id") in custom_ids:
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
    rules_formal = _build_formal_rules_index()

    # Build status per dataset+rule: ok/fail/na
    status: Dict[Tuple[str, str], str] = {}
    for ds in datasets:
        for rid in rules_formal:
            status[(ds, rid)] = "na"
    for it in items:
        rid = it.get("rule_id"); ds = it.get("dataset"); ok = it.get("success", False)
        if rid in rules_formal and ds:
            # if multiple results for same pair exist: any fail dominates
            prev = status.get((ds, rid), "na")
            if prev == "fail":
                continue
            status[(ds, rid)] = "ok" if ok else "fail"

    cells = [{"dataset": ds, "rule_id": rid, "status": status[(ds, rid)]}
             for ds in datasets for rid in rules_formal]

    custom_checks = _build_custom_checks_map(items)

    cov = {
        "tables_total": None,  # optional: set later from config
        "tables_validated": len(datasets),
        "datasets": datasets,
        "rules_formal": rules_formal,
        "cells": cells,
        "custom_checks": custom_checks
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
