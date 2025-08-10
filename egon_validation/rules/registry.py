from typing import Any, Dict, Iterable, List, Optional, Tuple, Type
from .base import Rule

# Internal registry: (rule_id, task, dataset, rule_cls, defaults, kind)
_REGISTRY: List[Tuple[str, str, str, Type[Rule], Dict[str, Any], str]] = []

def register(*, task: str, dataset: str, rule_id: str = None, kind: str = "formal", **default_params):
    def _decorator(rule_cls: Type[Rule]):
        rid = rule_id or rule_cls.__name__
        _REGISTRY.append((rid, task, dataset, rule_cls, default_params, kind))
        return rule_cls
    return _decorator

def rules_for(task: str, scenario: Optional[str]) -> Iterable[Rule]:
    for rid, tid, ds, cls, params, kind in _REGISTRY:
        if tid == task:
            # pass kind as param too (useful for coverage aggregation)
            inst = cls(rid, tid, ds, **{**params, "kind": kind})
            # scenario filtering is handled by each rule via `scenario_col` + ctx.scenario
            yield inst

def list_registered() -> List[Dict[str, Any]]:
    return [
        {"rule_id": rid, "task": tid, "dataset": ds, "kind": kind, "params": params}
        for rid, tid, ds, _, params, kind in _REGISTRY
    ]
