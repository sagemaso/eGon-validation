import json, os, time
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
from egon_validation.rules.registry import rules_for
from egon_validation.rules.base import SqlRule, RuleResult, Rule
from egon_validation import db
from egon_validation.logging_config import get_logger
import egon_validation.rules.formal  # noqa: F401
import egon_validation.rules.custom  # noqa: F401
import egon_validation.rules.custom.sanity   # noqa: F401

logger = get_logger("runner.execute")


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
            
            # Only pass scenario parameter if rule has both scenario_col AND scenario parameters
            params = {}
            if (hasattr(rule, 'params') and 
                rule.params.get('scenario_col') and 
                rule.params.get('scenario')):
                params["scenario"] = rule.params["scenario"]
            
            row = db.fetch_one(engine, rule.sql(ctx), params or None)
            res = rule.postprocess(row, ctx)
        else:
            res = rule.evaluate(engine, ctx)  # type: ignore
        execution_time = time.time() - start_time
        logger.debug("Rule completed successfully", extra={
            "rule_id": rule.rule_id,
            "execution_time_seconds": round(execution_time, 2),
            "success": res.success,
            "dataset": rule.dataset
        })
        return res
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error("Rule execution failed", extra={
            "rule_id": rule.rule_id,
            "execution_time_seconds": round(execution_time, 2),
            "error": str(e),
            "dataset": rule.dataset
        })
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
    logger.info("Starting rule execution", extra={
        "task": task,
        "total_rules": len(rules),
        "max_workers": max_workers,
        "run_id": ctx.run_id
    })
    
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
                    logger.error("Failed to retrieve rule result", extra={
                        "rule_id": rule.rule_id,
                        "error": str(e)
                    })
    
    total_time = time.time() - overall_start
    avg_time = total_time / len(results) if results else 0
    logger.info("Rule execution completed", extra={
        "task": task,
        "total_rules_completed": len(results),
        "total_execution_time_seconds": round(total_time, 2),
        "average_time_per_rule_seconds": round(avg_time, 2),
        "run_id": ctx.run_id,
        "output_path": jsonl_path
    })
    return results
