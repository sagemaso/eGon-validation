import json
import os
import time
from datetime import datetime
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
from egon_validation.rules.registry import rules_for
from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation import db
from egon_validation.exceptions import (
    RuleExecutionError,
    ValidationTimeoutError,
    RunIdCollisionError,
)
from egon_validation.logging_config import get_logger
from sqlalchemy.exc import SQLAlchemyError, OperationalError, TimeoutError
import egon_validation.rules.formal  # noqa: F401
import egon_validation.rules.custom  # noqa: F401

logger = get_logger("runner")


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
                execution_time = time.time() - start_time
                empty_result.execution_time = execution_time
                empty_result.executed_at = datetime.now().isoformat()
                return empty_result

            row = db.fetch_one(engine, rule.sql(ctx))
            res = rule.postprocess(row, ctx)
        else:
            res = rule.evaluate(engine, ctx)  # type: ignore
        execution_time = time.time() - start_time
        res.execution_time = execution_time
        res.executed_at = datetime.now().isoformat()
        # Set rule class name if not already set
        if not res.rule_class:
            res.rule_class = rule.__class__.__name__
        logger.info(
            f"Rule {rule.rule_id} completed in {execution_time:.2f}s",
            extra={
                "rule_id": rule.rule_id,
                "execution_time": execution_time,
                "status": "success",
            },
        )
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
            table=rule.table,
            success=False,
            observed=None,
            expected=None,
            message=f"Database error: {str(e)}",
            severity=severity,
            execution_time=execution_time,
            executed_at=datetime.now().isoformat(),
            schema=getattr(rule, "schema", None),
            table_name=getattr(rule, "table_name", None),
            kind=getattr(rule, "kind", "unknown"),
            rule_class=rule.__class__.__name__,
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
            table=rule.table,
            success=False,
            observed=None,
            expected=None,
            message=f"Rule execution error: {str(e)}",
            severity=Severity.ERROR,
            execution_time=execution_time,
            executed_at=datetime.now().isoformat(),
            schema=getattr(rule, "schema", None),
            table_name=getattr(rule, "table_name", None),
            kind=getattr(rule, "kind", "unknown"),
            rule_class=rule.__class__.__name__,
        )
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(
            f"Rule {rule.rule_id} unexpected error in {execution_time:.2f}s: {str(e)}",
            extra={
                "rule_id": rule.rule_id,
                "execution_time": execution_time,
                "error": str(e),
            },
            exc_info=True,
        )
        return RuleResult(
            rule_id=rule.rule_id,
            task=rule.task,
            table=rule.table,
            success=False,
            observed=None,
            expected=None,
            message=f"Unexpected error: {str(e)}",
            severity=Severity.ERROR,
            execution_time=execution_time,
            executed_at=datetime.now().isoformat(),
            schema=getattr(rule, "schema", None),
            table_name=getattr(rule, "table_name", None),
            kind=getattr(rule, "kind", "unknown"),
            rule_class=rule.__class__.__name__,
        )


def run_validations(
    engine, ctx, validations: List, task_name: str, max_workers: int = 6
) -> List[RuleResult]:
    """Execute a list of validation rule instances.

    This is the new entry point for inline validation execution.
    It accepts Rule instances directly instead of looking them up in the registry.

    Args:
        engine: SQLAlchemy engine
        ctx: Run context
        validations: List of instantiated Rule objects
        task_name: Name of the validation task (for context/output dir)
        max_workers: Number of parallel workers

    Returns:
        List of RuleResult objects
    """
    overall_start = time.time()
    results: List[RuleResult] = []

    # Set task name on all validation instances
    for validation in validations:
        validation.task = task_name

    # Create base task directory with timestamp
    task_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    task_dir = os.path.join(ctx.out_dir, ctx.run_id, "tasks", f"{task_name}")
    _ensure_dir(task_dir, check_collision=False)

    # Save expected rules for this task before execution
    expected_rules_file = os.path.join(task_dir, "expected_rules.json")
    expected_rules = [
        {
            "rule_id": v.rule_id,
            "table": v.table,
            "kind": getattr(v, "kind", "unknown"),
            "rule_class": v.__class__.__name__
        }
        for v in validations
    ]
    with open(expected_rules_file, "w") as f:
        json.dump(expected_rules, f, indent=2)

    logger.debug(
        f"Saved {len(expected_rules)} expected rules to {expected_rules_file}",
        extra={"task": task_name, "expected_rules_count": len(expected_rules)}
    )

    logger.info(
        f"Executing {len(validations)} validations for task '{task_name}' (max_workers={max_workers})",
        extra={"task": task_name, "rule_count": len(validations), "max_workers": max_workers},
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all rules for execution
        future_to_rule = {
            executor.submit(_execute_single_rule, engine, rule, ctx): rule
            for rule in validations
        }

        # Collect results as they complete and write to per-rule file
        for future in as_completed(future_to_rule):
            rule = future_to_rule[future]
            try:
                res = future.result()
                results.append(res)

                # Create per-rule directory and append to its JSONL file
                rule_dir = os.path.join(task_dir, rule.rule_id)
                _ensure_dir(rule_dir, check_collision=False)
                jsonl_path = os.path.join(rule_dir, "results.jsonl")

                with open(jsonl_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(res.to_dict()) + "\n")
            except Exception as e:
                logger.error(
                    f"Failed to get result for rule {rule.rule_id}: {e}",
                    extra={"rule_id": rule.rule_id, "task": task_name, "error": str(e)},
                    exc_info=True,
                )

    total_time = time.time() - overall_start
    avg_time = total_time / len(results) if results else 0
    logger.info(
        f"Completed {len(results)} validations in {total_time:.2f}s "
        f"(avg: {avg_time:.2f}s per rule)",
        extra={
            "task": task_name,
            "total_time": total_time,
            "avg_time": avg_time,
            "completed_rules": len(results),
        },
    )
    return results


def run_for_task(engine, ctx, task: str, max_workers: int = 6) -> List[RuleResult]:
    """Execute rules registered for a task (legacy registry-based approach).

    This function uses the decorator-based registry to look up rules.
    It's kept for backward compatibility with CLI usage.

    Args:
        engine: SQLAlchemy engine
        ctx: Run context
        task: Task name to look up in registry
        max_workers: Number of parallel workers

    Returns:
        List of RuleResult objects
    """
    rules = list(rules_for(task))
    return run_validations(engine, ctx, rules, task, max_workers)
