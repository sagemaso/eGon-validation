"""Base classes for validation rules: Rule, SqlRule, RuleResult, and Severity enum."""

from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any, Dict, Optional


# PostgreSQL type mappings for data type validation
POSTGRES_TYPE_MAPPINGS = {
    "integer": [
        "integer",
        "int4",
        "int",
        "bigint",
        "int8",
        "smallint",
        "int2",
    ],
    "text": ["text", "character varying", "varchar", "character", "char"],
    "numeric": [
        "numeric",
        "decimal",
        "real",
        "double precision",
        "float4",
        "float8",
    ],
    "boolean": ["boolean", "bool"],
    "timestamp": [
        "timestamp without time zone",
        "timestamp with time zone",
        "timestamptz",
    ],
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
    table: str
    kind: str
    success: bool
    message: str = ""
    observed: Optional[float] = None
    expected: Optional[float] = None
    severity: Severity = None
    execution_time: Optional[float] = None
    executed_at: Optional[str] = None  # ISO timestamp when rule was executed
    rule_class: Optional[str] = (
        None  # Class name of the rule (e.g., "ArrayCardinalityValidation")
    )
    # Debug fields
    schema: Optional[str] = None
    table_name: Optional[str] = None
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
    def __init__(
        self,
        rule_id: str,
        table: str,
        task: Optional[str] = None,
        message_suffix: Optional[str] = None,
        **params: Any,
    ) -> None:
        self.rule_id = rule_id
        self.task = task or ""  # Can be set later by run_validations
        self.kind = self._infer_kind_from_module()
        self.table = table  # "<schema>.<table>" or view
        self.message_suffix = message_suffix
        self.params: Dict[str, Any] = params
        # Parse schema and table_name for debug/filtering
        self.schema, self.table_name = self._parse_table_name(table)

    def _infer_kind_from_module(self) -> str:
        """
        Gets kind from module name.
        Expects pattern like: '...rules.custom.meine_regel'
        oder '...rules.formal.meine_regel'.
        """
        module_name = self.__class__.__module__
        marker = ".rules."
        if marker in module_name:
            after_rules = module_name.split(marker, 1)[1]
            subpackage = after_rules.split(".", 1)[0]

            if subpackage in {"custom", "formal"}:
                return subpackage

        return "unknown"

    @staticmethod
    def _parse_table_name(table: str) -> tuple[Optional[str], str]:
        """Parse table string into (schema, table_name).

        Args:
            table: Table identifier, either "table" or "schema.table"

        Returns:
            Tuple of (schema, table_name). Schema is None if not provided.

        Example:
            >>> Rule._parse_table_name("myschema.mytable")
            ("myschema", "mytable")
            >>> Rule._parse_table_name("mytable")
            (None, "mytable")
        """
        if "." in table:
            parts = table.split(".", 1)
            return parts[0], parts[1]
        return None, table

    def _build_message(self, base_message: str) -> str:
        """Append message_suffix to base message if configured.

        Args:
            base_message: The original validation message

        Returns:
            Message with suffix appended if message_suffix is set
        """
        if self.message_suffix:
            return f"{base_message} | {self.message_suffix}"
        return base_message

    def create_result(
        self,
        success: bool,
        message: str = "",
        observed: Optional[float] = None,
        expected: Optional[Any] = None,
        severity: Optional[Severity] = None,
        **kwargs,
    ) -> RuleResult:
        """Factory method for RuleResult with common fields pre-filled.

        Args:
            success: Whether the validation passed
            message: Validation message
            observed: Observed value
            expected: Expected value
            severity: Result severity (auto-set if None)
            **kwargs: Additional fields for RuleResult

        Returns:
            RuleResult with all common fields populated
        """
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            message=self._build_message(message),
            observed=observed,
            expected=expected,
            severity=severity,
            schema=self.schema,
            table_name=self.table_name,
            **kwargs,
        )

    def empty_table_result(self, query: str = None) -> RuleResult:
        """Standard result for empty tables or query results.

        Args:
            query: Optional SQL query that returned no results

        Returns:
            RuleResult indicating table/query is empty
        """
        if query:
            # Extract WHERE clause to show filters
            where_clause = ""
            if "WHERE" in query.upper():
                where_clause = (
                    query.upper()
                    .split("WHERE", 1)[1]
                    .split("ORDER BY")[0]
                    .split("GROUP BY")[0]
                    .strip()
                )

            if where_clause:
                message = f"⚠️ NO DATA FOUND: No rows in {self.table} match query filters\n   Query: {query[:200]}..."
            else:
                message = f"⚠️ EMPTY TABLE: {self.table} has no data"
        else:
            message = f"⚠️ EMPTY TABLE: {self.table} has no data to validate"

        return self.create_result(
            success=False, observed=0, expected=">0", message=message
        )

    def error_result(self, message: str, **kwargs) -> RuleResult:
        """Create an error RuleResult with ERROR severity.

        Args:
            message: Error message
            **kwargs: Additional fields to pass to RuleResult

        Returns:
            RuleResult with success=False and severity=ERROR
        """
        return self.create_result(
            success=False, message=message, severity=Severity.ERROR, **kwargs
        )

    def get_schema_and_table(self) -> tuple[str, str]:
        """Parse table into schema and table name.

        Returns:
            tuple: (schema, table)

        Raises:
            ValueError: If table does not contain a schema (missing '.')
        """
        if "." not in self.table:
            raise ValueError(
                f"Table '{self.table}' must include schema in format 'schema.table'"
            )
        return tuple(self.table.split(".", 1))

    @staticmethod
    def severity_from_success(
        success: bool, error_severity: Severity = Severity.ERROR
    ) -> Severity:
        """Determine severity based on validation success.

        Args:
            success: Whether validation passed
            error_severity: Severity to use on failure (default: ERROR)

        Returns:
            Severity.INFO if success, otherwise error_severity
        """
        return Severity.INFO if success else error_severity

    @staticmethod
    def within_tolerance(
        actual: float, expected: float, tolerance: float = 0.0
    ) -> bool:
        """Check if actual value is within tolerance of expected value.

        Args:
            actual: The observed/actual value
            expected: The expected value
            tolerance: Relative tolerance (e.g., 0.01 for 1%)

        Returns:
            True if |actual - expected| <= expected * tolerance
        """
        return abs(actual - expected) <= (expected * tolerance)


class SqlRule(Rule):
    def sql(self, ctx) -> str:
        raise NotImplementedError

    def postprocess(self, row: Dict[str, Any], ctx) -> RuleResult:
        raise NotImplementedError

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

    def _check_table_empty(self, engine, ctx) -> Optional[RuleResult]:
        """Check if the table is empty and return failure result if so."""
        try:
            # Build the count query with same scenario filtering as main query
            count_query = f"SELECT COUNT(*) as total_count FROM {self.table}"

            from egon_validation import db

            count_row = db.fetch_one(engine, count_query)
            total_count = int(count_row.get("total_count", 0))

            if total_count == 0:
                return self.empty_table_result()

            return None  # Table has data, continue normal validation

        except Exception:
            # If we can't check the table, let the main query handle it
            return None


class DataFrameRule(Rule):
    """Base class for DataFrame-based validation rules."""

    def get_query(self, ctx) -> str:
        """Generate SQL query for DataFrame creation. Override in subclasses."""
        return f"SELECT * FROM {self.table}"

    def evaluate_df(self, df, ctx) -> RuleResult:
        """Perform validation on DataFrame. Override in subclasses."""
        raise NotImplementedError

    def evaluate(self, engine, ctx) -> RuleResult:
        """Execute rule by fetching DataFrame and calling evaluate_df."""
        try:
            from egon_validation.db import fetch_dataframe

            # Get DataFrame
            query = self.get_query(ctx)
            df = fetch_dataframe(engine, query)

            # Check if query returned no results
            if df.empty:
                return self.empty_table_result(query=query)

            # Delegate to DataFrame-specific evaluation
            return self.evaluate_df(df, ctx)

        except Exception as e:
            return self.error_result(
                message=f"DataFrame rule execution failed: {str(e)}"
            )
