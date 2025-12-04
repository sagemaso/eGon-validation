"""Rule registration and discovery system."""

from typing import Any, Dict, Iterable, List, Optional, Tuple, Type
from .base import Rule

# Internal registry: (rule_id, task, table, rule_cls, defaults)
_REGISTRY: List[
    Tuple[str, str, str, Type[Rule], Dict[str, Any]]
] = []


def register(
    *,
    task: str,
    table: str,
    rule_id: str = None,
    **default_params,
):
    """Decorator to register a validation rule."""

    def _decorator(rule_cls: Type[Rule]):
        rid = rule_id or rule_cls.__name__
        params = dict(default_params)
        _REGISTRY.append((rid, task, table, rule_cls, params))
        return rule_cls

    return _decorator


def register_map(
    *,
    task: str,
    rule_cls,
    rule_id: str = None,
    tables_params: dict,
):
    """Register one rule for multiple tables."""
    rid = rule_id or rule_cls.__name__
    for tbl, params in tables_params.items():
        p = dict(params)
        _REGISTRY.append((rid, task, tbl, rule_cls, p))


def rules_for(task: str) -> Iterable[Rule]:
    """Get all rules registered for task."""
    for rid, tid, tbl, cls, params in _REGISTRY:
        if tid == task:
            inst = cls(rid, tbl, tid, **params)
            yield inst


def list_registered() -> List[Dict[str, Any]]:
    """List all registered rules."""
    result: List[Dict[str, Any]] = []

    for rid, tid, tbl, cls, params in _REGISTRY:
        rule = cls(
            rule_id=rid,
            table=tbl,
            task=tid,
            **params,
        )

        result.append(
            {
                "rule_id": rid,
                "task": tid,
                "table": tbl,
                "kind": rule.kind,
                "params": params,
            }
        )

    return result
