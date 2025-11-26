from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register
from egon_validation.config import MV_GRID_DISTRICTS_COUNT


@register(
    task="validation-test",
    table="grid.egon_mv_grid_district",
    rule_id="MV_GRID_DISTRICT_COUNT",
    kind="formal",
    expected_count=3854,
)
class RowCountValidation(SqlRule):
    """Validates that a table has the expected number of rows.

    Args:
        rule_id: Unique identifier for this validation rule
        task: Task identifier
        table: Full table name including schema (e.g., "schema.table")
        expected_count: Expected number of rows in the table (passed in params)
        kind: Validation kind (passed in params, default: "formal")

    Example:
        >>> validation = RowCountValidation(
        ...     rule_id="VG250_ROW_CHECK",
        ...     table="boundaries.vg250_krs",
        ...     expected_count=27
        ... )
    """

    def sql(self, ctx):
        return f"SELECT COUNT(*) AS actual_count FROM {self.table}"

    def postprocess(self, row, ctx):
        actual_count = int(row.get("actual_count") or 0)
        expected_count = int(self.params.get("expected_count", MV_GRID_DISTRICTS_COUNT))

        ok = actual_count == expected_count

        message = f"Expected {expected_count} rows, found {actual_count}"

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            success=ok,
            observed=float(actual_count),
            expected=float(expected_count),
            message=message,
            schema=self.schema,
            table_name=self.table_name,
        )
