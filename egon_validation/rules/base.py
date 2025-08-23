from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any, Dict, Optional

class Severity(Enum):
    INFO = "INFO"
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
    
    def _check_table_empty(self, engine, ctx) -> Optional[RuleResult]:
        """Check if the table is empty and return failure result if so."""
        try:
            # Build the count query with same scenario filtering as main query
            count_query = f"SELECT COUNT(*) as total_count FROM {self.dataset}"
            scenario_col = self.params.get("scenario_col")
            scenario = self.params.get("scenario")
            
            if scenario_col and scenario:
                count_query += f" WHERE {scenario_col} = :scenario"
                params = {"scenario": scenario}
            else:
                params = {}
            
            from egon_validation import db
            count_row = db.fetch_one(engine, count_query, params)
            total_count = int(count_row.get("total_count", 0))
            
            if total_count == 0:
                scenario_info = f" (scenario: {scenario})" if scenario else ""
                return RuleResult(
                    rule_id=self.rule_id, task=self.task, dataset=self.dataset,
                    success=False, observed=0, expected=">0",
                    message=f"🚨 EMPTY TABLE: {self.dataset} has no data to validate{scenario_info}",
                    severity=Severity.INFO, schema=self.schema, table=self.table
                )
            
            return None  # Table has data, continue normal validation
            
        except Exception as e:
            # If we can't check the table, let the main query handle it
            return None
