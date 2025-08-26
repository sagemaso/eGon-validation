import json, os, time
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
from egon_validation.rules.registry import rules_for
from egon_validation.rules.base import SqlRule, RuleResult, Rule
from egon_validation import db
import egon_validation.rules.formal  # noqa: F401
import egon_validation.rules.custom  # noqa: F401


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _execute_single_rule(engine, rule, ctx) -> RuleResult:
    """Execute a single rule and return the result."""
    start_time = time.time()
    try:
        if isinstance(rule, SqlRule):
            # Check if table is empty first
            empty_result = rule._check_table_empty(engine, ctx)
            if empty_result:
                return empty_result

            row = db.fetch_one(engine, rule.sql(ctx))
            res = rule.postprocess(row, ctx)
        else:
            res = rule.evaluate(engine, ctx)  # type: ignore
        execution_time = time.time() - start_time
        print(f"Rule {rule.rule_id} completed in {execution_time:.2f}s")
        return res
    except Exception as e:
        execution_time = time.time() - start_time
        print(f"Rule {rule.rule_id} failed in {execution_time:.2f}s: {str(e)}")
        from egon_validation.rules.base import RuleResult, Severity
        return RuleResult(
            rule_id=rule.rule_id, task=rule.task, dataset=rule.dataset,
            success=False, observed=None, expected=None,
            message=f"Rule execution failed: {str(e)}", severity=Severity.ERROR,
            schema=getattr(rule, 'schema', None), table=getattr(rule, 'table', None)
        )

def run_for_task(engine, ctx, task: str, max_workers: int = 4) -> List[RuleResult]:
    overall_start = time.time()
    results: List[RuleResult] = []
    out_dir = os.path.join(ctx.out_dir, ctx.run_id, "tasks", task)
    _ensure_dir(out_dir)
    jsonl_path = os.path.join(out_dir, "results.jsonl")
    
    rules = list(rules_for(task))
    print(f"Executing {len(rules)} rules in parallel (max_workers={max_workers})...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all rules for execution
        future_to_rule = {
            executor.submit(_execute_single_rule, engine, rule, ctx): rule 
            for rule in rules
        }
        
        # Collect results as they complete and write to file
        with open(jsonl_path, "a", encoding="utf-8") as f:
            for future in as_completed(future_to_rule):
                rule = future_to_rule[future]
                try:
                    res = future.result()
                    results.append(res)
                    f.write(json.dumps(res.to_dict()) + "\n")
                except Exception as e:
                    print(f"Failed to get result for rule {rule.rule_id}: {e}")
    
    total_time = time.time() - overall_start
    print(f"Completed {len(results)} rules in {total_time:.2f}s (avg: {total_time/len(results):.2f}s per rule)")
    return results
