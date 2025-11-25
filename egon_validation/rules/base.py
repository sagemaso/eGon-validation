"""Base classes for validation rules: Rule, SqlRule, RuleResult, and Severity enum."""

from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any, Dict, Optional


# PostgreSQL type mappings for data type validation
POSTGRES_TYPE_MAPPINGS = {
    "integer": ["integer", "int4", "int", "bigint", "int8", "smallint", "int2"],
    "text": ["text", "character varying", "varchar", "character", "char"],
    "numeric": ["numeric", "decimal", "real", "double precision", "float4", "float8"],
    "boolean": ["boolean", "bool"],
    "timestamp": ["timestamp without time zone", "timestamp with time zone", "timestamptz"],
    "date": ["date"],
    "uuid": ["uuid"],
    "geometry": ["geometry", "geography"],
    "array": ["array", "_int4", "_text", "_numeric"],
}


class Severity(Enum):
    """Severity levels: INFO, WARNING, ERROR."""

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
    severity: Severity = None
    execution_time: Optional[float] = None
    executed_at: Optional[str] = None  # ISO timestamp when rule was executed
    # Debug fields
    schema: Optional[str] = None
    table: Optional[str] = None
    column: Optional[str] = None

    def __post_init__(self):
        """Auto-set severity based on success if not explicitly provided."""
        if self.severity is None:
            self.severity = Severity.INFO if self.success else Severity.WARNING

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



class SqlRule(Rule):
    def sql(self, ctx) -> str:
        raise NotImplementedError

    def postprocess(self, row: Dict[str, Any], ctx) -> RuleResult:
        raise NotImplementedError

    def get_schema_and_table(self) -> tuple[str, str]:
        """Parse dataset into schema and table name.

        Returns:
            tuple: (schema, table)

        Raises:
            ValueError: If dataset does not contain a schema (missing '.')
        """
        if "." not in self.dataset:
            raise ValueError(
                f"Dataset '{self.dataset}' must include schema in format 'schema.table'"
            )
        return self.dataset.split(".", 1)

    @staticmethod
    def parse_json_result(json_data):
        """Parse JSON data that may be a string or already parsed.

        Args:
            json_data: JSON data either as string or already parsed dict/list

        Returns:
            Parsed JSON data (dict or list)
        """
        import json
        if isinstance(json_data, str):
            return json.loads(json_data)
        return json_data

    def error_result(self, message: str, **kwargs) -> RuleResult:
        """Create an error RuleResult with ERROR severity.

        Args:
            message: Error message
            **kwargs: Additional fields to pass to RuleResult

        Returns:
            RuleResult with success=False and severity=ERROR
        """
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            dataset=self.dataset,
            success=False,
            message=message,
            severity=Severity.ERROR,
            schema=self.schema,
            table=self.table,
            **kwargs
        )

    def _check_table_empty(self, engine, ctx) -> Optional[RuleResult]:
        """Check if the table is empty and return failure result if so."""
        try:
            # Build the count query with same scenario filtering as main query
            count_query = f"SELECT COUNT(*) as total_count FROM {self.dataset}"

            from egon_validation import db

            count_row = db.fetch_one(engine, count_query)
            total_count = int(count_row.get("total_count", 0))

            if total_count == 0:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    dataset=self.dataset,
                    success=False,
                    observed=0,
                    expected=">0",
                    message=f"🚨 EMPTY TABLE: {self.dataset} has no data to validate",
                    schema=self.schema,
                    table=self.table,
                )

            return None  # Table has data, continue normal validation

        except Exception:
            # If we can't check the table, let the main query handle it
            return None


class DataFrameRule(Rule):
    """Base class for DataFrame-based validation rules."""

    def get_query(self, ctx) -> str:
        """Generate SQL query for DataFrame creation. Override in subclasses."""
        return f"SELECT * FROM {self.dataset}"

    def evaluate_df(self, df, ctx) -> RuleResult:
        """Perform validation on DataFrame. Override in subclasses."""
        raise NotImplementedError

    def evaluate(self, engine, ctx) -> RuleResult:
        """Execute rule by fetching DataFrame and calling evaluate_df."""
        try:
            from egon_validation.db import fetch_dataframe

            # Get DataFrame
            df = fetch_dataframe(engine, self.get_query(ctx))

            # Check if table is empty
            if df.empty:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    dataset=self.dataset,
                    success=False,
                    observed=0,
                    expected=">0",
                    message=f"🚨 EMPTY TABLE: {self.dataset} has no data to validate",
                    schema=self.schema,
                    table=self.table,
                )

            # Delegate to DataFrame-specific evaluation
            return self.evaluate_df(df, ctx)

        except Exception as e:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                dataset=self.dataset,
                success=False,
                observed=None,
                expected=None,
                message=f"DataFrame rule execution failed: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table=self.table,
            )
