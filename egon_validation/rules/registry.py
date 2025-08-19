from typing import Any, Dict, Iterable, List, Optional, Tuple, Type
from .base import Rule

# Internal registry: (rule_id, task, dataset, rule_cls, defaults, kind, scenario)
_REGISTRY: List[Tuple[str, str, str, Type[Rule], Dict[str, Any], str, Optional[str]]] = []

def register(*, task: str, dataset: str, rule_id: str = None, kind: str = "formal", scenario: Optional[str] = None, **default_params):
    def _decorator(rule_cls: Type[Rule]):
        rid = rule_id or rule_cls.__name__
        _REGISTRY.append((rid, task, dataset, rule_cls, default_params, kind, scenario))
        return rule_cls
    return _decorator

def register_map(*, task: str, rule_cls, rule_id: str = None,
                 kind: str = "formal", scenario: Optional[str] = None, datasets_params: dict):
    rid = rule_id or rule_cls.__name__
    for ds, params in datasets_params.items():
        p = dict(params)
        _REGISTRY.append((rid, task, ds, rule_cls, p, kind, scenario))

def rules_for(task: str) -> Iterable[Rule]:
    for rid, tid, ds, cls, params, kind, rule_scenario in _REGISTRY:
        if tid == task:
            # pass kind as param too (useful for coverage aggregation)
            inst = cls(rid, tid, ds, **{**params, "kind": kind})
            yield inst

def list_registered() -> List[Dict[str, Any]]:
    return [
        {"rule_id": rid, "task": tid, "dataset": ds, "kind": kind, "params": params, "scenario": scenario}
        for rid, tid, ds, _, params, kind, scenario in _REGISTRY
    ]
