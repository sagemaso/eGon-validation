"""Rule registration and discovery system."""

from typing import Any, Dict, Iterable, List, Optional, Tuple, Type
from .base import Rule

# Internal registry: (rule_id, task, table, rule_cls, defaults, kind)
_REGISTRY: List[
    Tuple[str, str, str, Type[Rule], Dict[str, Any], str]
] = []


def register(
    *,
    task: str,
    table: str,
    rule_id: str = None,
    kind: str = "formal",
    **default_params,
):
    """Decorator to register a validation rule."""

    def _decorator(rule_cls: Type[Rule]):
        rid = rule_id or rule_cls.__name__
        _REGISTRY.append((rid, task, table, rule_cls, default_params, kind))
        return rule_cls

    return _decorator


def register_map(
    *,
    task: str,
    rule_cls,
    rule_id: str = None,
    kind: str = "formal",
    tables_params: dict,
):
    """Register one rule for multiple tables."""
    rid = rule_id or rule_cls.__name__
    for tbl, params in tables_params.items():
        p = dict(params)
        _REGISTRY.append((rid, task, tbl, rule_cls, p, kind))


def rules_for(task: str) -> Iterable[Rule]:
    """Get all rules registered for task."""
    for rid, tid, tbl, cls, params, kind in _REGISTRY:
        if tid == task:
            # pass kind as param too (useful for coverage aggregation)
            inst = cls(rid, tbl, tid, **{**params, "kind": kind})
            yield inst


def list_registered() -> List[Dict[str, Any]]:
    """List all registered rules."""
    return [
        {"rule_id": rid, "task": tid, "table": tbl, "kind": kind, "params": params}
        for rid, tid, tbl, _, params, kind in _REGISTRY
    ]
