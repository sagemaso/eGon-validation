from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any, Dict, Optional

class Severity(Enum):
    WARNING = "WARNING"
    ERROR = "ERROR"

@dataclass
class RuleResult:
    rule_id: str
    task: str
    dataset: str
    success: bool
    message: str = ""
    observed: Optional[float] = None
    expected: Optional[float] = None
    severity: Severity = Severity.WARNING
    # Debug fields
    schema: Optional[str] = None
    table: Optional[str] = None
    column: Optional[str] = None

    def to_dict(self):
        d = asdict(self)
        # Enum -> String für JSON
        d["severity"] = self.severity.value if self.severity else None
        return d

class Rule:
    def __init__(self, rule_id: str, task: str, dataset: str, **params: Any) -> None:
        self.rule_id = rule_id
        self.task = task
        self.dataset = dataset  # "<schema>.<table>" or view
        self.params: Dict[str, Any] = params
        # derive schema/table for debug (best-effort)
        if "." in dataset:
            self.schema, self.table = dataset.split(".", 1)
        else:
            self.schema, self.table = None, dataset

    def evaluate(self, engine, ctx) -> RuleResult:
        raise NotImplementedError

class SqlRule(Rule):
    def sql(self, ctx) -> str:
        raise NotImplementedError

    def postprocess(self, row: Dict[str, Any], ctx) -> RuleResult:
        raise NotImplementedError
