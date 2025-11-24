from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register
from egon_validation.config import MV_GRID_DISTRICTS_COUNT


@register(
    task="validation-test",
    dataset="grid.egon_mv_grid_district",
    rule_id="MV_GRID_DISTRICT_COUNT",
    kind="formal",
    expected_count=3854,
)
class RowCountValidation(SqlRule):
    """Validates that a table has the expected number of rows.

    This validation can be used in two ways:
    1. Decorator-based registration (legacy, for CLI usage)
    2. Direct instantiation (new, for Airflow inline declaration)

    Args:
        table: Full table name including schema (e.g., "schema.table")
        rule_id: Unique identifier for this validation rule
        expected_count: Expected number of rows in the table
        kind: Validation kind - "formal" or "custom" (default: "formal")

    Example (inline declaration):
        >>> validation = RowCountValidation(
        ...     table="boundaries.vg250_krs",
        ...     rule_id="VG250_ROW_CHECK",
        ...     expected_count=27
        ... )
    """

    def __init__(self, table: str, rule_id: str, expected_count: int,
                 kind: str = "formal"):
        """Initialize row count validation.

        Args:
            table: Full table name (schema.table)
            rule_id: Unique identifier
            expected_count: Expected row count
            kind: Validation type (default: "formal")
        """
        super().__init__(
            rule_id=rule_id,
            task="inline",  # Will be set by executor
            dataset=table,
            expected_count=expected_count,
            kind=kind
        )

    def sql(self, ctx):
        return f"SELECT COUNT(*) AS actual_count FROM {self.dataset}"

    def postprocess(self, row, ctx):
        actual_count = int(row.get("actual_count") or 0)
        expected_count = int(self.params.get("expected_count", MV_GRID_DISTRICTS_COUNT))

        ok = actual_count == expected_count

        message = f"Expected {expected_count} rows, found {actual_count}"

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            dataset=self.dataset,
            success=ok,
            observed=float(actual_count),
            expected=float(expected_count),
            message=message,
            schema=self.schema,
            table=self.table,
        )


@register(
    task="validation-test",
    dataset="demand.egon_demandregio_cts_ind",
    rule_id="CTS_IND_ROW_COUNT_MATCH",
    kind="formal",
    scenario_col="scenario",
    economic_sector_col="wz",
    reference_dataset="boundaries.vg250_krs",
    reference_filter="gf = 4",
)
class RowCountComparisonValidation(SqlRule):
    """Validates that grouped row counts match a reference table count.

    Args:
        table: Table to validate (child table)
        rule_id: Unique identifier
        reference_table: Reference table to compare against
        reference_filter: SQL WHERE clause for reference table (default: "TRUE")
        group_by_cols: List of column names to group by
        kind: Validation kind (default: "formal")

    Example:
        >>> validation = RowCountComparisonValidation(
        ...     table="demand.egon_demandregio_cts_ind",
        ...     rule_id="CTS_IND_ROW_COUNT_MATCH",
        ...     reference_table="boundaries.vg250_krs",
        ...     reference_filter="gf = 4",
        ...     group_by_cols=["scenario", "wz"]
        ... )
    """

    def __init__(self, table: str, rule_id: str, reference_table: str,
                 reference_filter: str = "TRUE", group_by_cols: list = None,
                 kind: str = "formal"):
        """Initialize row count comparison validation."""
        group_by_cols = group_by_cols or ["scenario", "wz"]
        super().__init__(
            rule_id=rule_id,
            task="inline",
            dataset=table,
            reference_dataset=reference_table,
            reference_filter=reference_filter,
            scenario_col=group_by_cols[0] if len(group_by_cols) > 0 else "scenario",
            economic_sector_col=group_by_cols[1] if len(group_by_cols) > 1 else "wz",
            kind=kind
        )

    def sql(self, ctx):
        reference_dataset = self.params.get("reference_dataset")
        reference_filter = self.params.get("reference_filter", "TRUE")
        scenario = self.params.get("scenario_col")
        economic_sector = self.params.get("economic_sector_col")

        base_query = f"""
        WITH reference_count AS (
            SELECT COUNT(*) AS ref_count FROM {reference_dataset} WHERE {reference_filter}
        ),
        grouped_counts AS (
            SELECT 
                {scenario},
                {economic_sector},
                COUNT(*) AS group_count
            FROM {self.dataset}
        """

        base_query += f"""
            GROUP BY {scenario}, {economic_sector}
        )
        SELECT 
            r.ref_count,
            COUNT(g.group_count) AS total_groups,
            COUNT(CASE WHEN g.group_count = r.ref_count THEN 1 END) AS matching_groups,
            COUNT(CASE WHEN g.group_count != r.ref_count THEN 1 END) AS mismatching_groups,
            array_agg(DISTINCT g.group_count) AS found_counts
        FROM reference_count r
        CROSS JOIN grouped_counts g
        GROUP BY r.ref_count
        """

        return base_query

    def postprocess(self, row, ctx):
        ref_count = int(row.get("ref_count") or 0)
        total_groups = int(row.get("total_groups") or 0)
        matching_groups = int(row.get("matching_groups") or 0)
        mismatching_groups = int(row.get("mismatching_groups") or 0)
        found_counts = row.get("found_counts", [])

        ok = mismatching_groups == 0

        if ok:
            message = f"All {total_groups} groups have expected count {ref_count}"
        else:
            message = f"{mismatching_groups}/{total_groups} groups have wrong count. Expected: {ref_count}, Found: {found_counts}"

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            dataset=self.dataset,
            success=ok,
            observed=float(mismatching_groups),
            expected=0.0,
            message=message,
            schema=self.schema,
            table=self.table,
        )
