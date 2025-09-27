import json
import os
import time
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
from egon_validation.rules.registry import rules_for
from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation import db
from egon_validation.logging_config import get_logger
from egon_validation.exceptions import (
    RuleExecutionError,
    ValidationTimeoutError,
    RunIdCollisionError,
)
from egon_validation.logging_config import get_logger
from sqlalchemy.exc import SQLAlchemyError, OperationalError, TimeoutError
import egon_validation.rules.formal  # noqa: F401
import egon_validation.rules.custom  # noqa: F401

logger = get_logger("runner.execute")



def _ensure_dir(path: str, check_collision: bool = True) -> None:
    """Create directory, optionally checking for run_id collisions."""
    if check_collision and os.path.exists(path):
        # Check if there are existing result files
        jsonl_files = [f for f in os.listdir(path) if f.endswith(".jsonl")]
        if jsonl_files:
            raise RunIdCollisionError(f"Run directory already exists with data: {path}")
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
        logger.info(
            f"Rule {rule.rule_id} completed in {execution_time:.2f}s",
            extra={
                "rule_id": rule.rule_id,
                "execution_time": execution_time,
                "status": "success",
            },
        )
        logger.debug("Rule completed successfully", extra={
            "rule_id": rule.rule_id,
            "execution_time_seconds": round(execution_time, 2),
            "success": res.success,
            "dataset": rule.dataset
        })
        return res
    except TimeoutError as e:
        execution_time = time.time() - start_time
        logger.error(
            f"Rule {rule.rule_id} timed out in {execution_time:.2f}s: {str(e)}",
            extra={
                "rule_id": rule.rule_id,
                "execution_time": execution_time,
                "error": str(e),
            },
        )
        raise ValidationTimeoutError(f"Rule {rule.rule_id} timed out: {str(e)}")
    except (OperationalError, SQLAlchemyError) as e:
        execution_time = time.time() - start_time
        logger.warning(
            f"Rule {rule.rule_id} database error in {execution_time:.2f}s: {str(e)}",
            extra={
                "rule_id": rule.rule_id,
                "execution_time": execution_time,
                "error": str(e),
            },
        )
        # Map database exceptions to appropriate severity
        severity = (
            Severity.ERROR if "connection" in str(e).lower() else Severity.WARNING
        )
        return RuleResult(
            rule_id=rule.rule_id,
            task=rule.task,
            dataset=rule.dataset,
            success=False,
            observed=None,
            expected=None,
            message=f"Database error: {str(e)}",
            severity=severity,
            schema=getattr(rule, "schema", None),
            table=getattr(rule, "table", None),
        )
    except RuleExecutionError as e:
        execution_time = time.time() - start_time
        logger.error(
            f"Rule {rule.rule_id} execution error in {execution_time:.2f}s: {str(e)}",
            extra={
                "rule_id": rule.rule_id,
                "execution_time": execution_time,
                "error": str(e),
            },
        )
        return RuleResult(
            rule_id=rule.rule_id,
            task=rule.task,
            dataset=rule.dataset,
            success=False,
            observed=None,
            expected=None,
            message=f"Rule execution error: {str(e)}",
            severity=Severity.ERROR,
            schema=getattr(rule, "schema", None),
            table=getattr(rule, "table", None),
        )
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
            rule_id=rule.rule_id,
            task=rule.task,
            dataset=rule.dataset,
            success=False,
            observed=None,
            expected=None,
            message=f"Unexpected error: {str(e)}",
            severity=Severity.ERROR,
            schema=getattr(rule, "schema", None),
            table=getattr(rule, "table", None),
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
    
    logger.info(
        f"Executing {len(rules)} rules in parallel (max_workers={max_workers})",
        extra={"task": task, "rule_count": len(rules), "max_workers": max_workers},
    )

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
                    logger.error(
                        f"Failed to get result for rule {rule.rule_id}: {e}",
                        extra={"rule_id": rule.rule_id, "task": task, "error": str(e)},
                        exc_info=True,
                    )

                    logger.error("Failed to retrieve rule result", extra={
                        "rule_id": rule.rule_id,
                        "error": str(e)
                    })

    total_time = time.time() - overall_start
    avg_time = total_time / len(results) if results else 0
    logger.info(
        f"Completed {len(results)} rules in {total_time:.2f}s "
        f"(avg: {avg_time:.2f}s per rule)",
        extra={
            "task": task,
            "total_time": total_time,
            "avg_time": avg_time,
            "completed_rules": len(results),
        },
    )
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
