import json, os
from typing import List
from egon_validation.rules.registry import rules_for
from egon_validation.rules.base import SqlRule, Rule, RuleResult
from egon_validation import db
import egon_validation.rules.formal  # noqa: F401
import egon_validation.rules.custom  # noqa: F401


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def run_for_task(engine, ctx, task: str) -> List[RuleResult]:
    results: List[RuleResult] = []
    out_dir = os.path.join(ctx.out_dir, ctx.run_id, "tasks", task)
    _ensure_dir(out_dir)
    jsonl_path = os.path.join(out_dir, "results.jsonl")
    with open(jsonl_path, "a", encoding="utf-8") as f:
        for rule in rules_for(task, ctx.scenario):
            if isinstance(rule, SqlRule):
                row = db.fetch_one(engine, rule.sql(ctx), {"scenario": ctx.scenario} if ctx.scenario else None)
                res = rule.postprocess(row, ctx)
            else:
                # PyRule or other kinds can implement evaluate directly
                res = rule.evaluate(engine, ctx)  # type: ignore
            results.append(res)
            f.write(json.dumps(res.to_dict()) + "\n")
    return results
