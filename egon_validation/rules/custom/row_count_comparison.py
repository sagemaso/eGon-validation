"""Custom validation for grouped row count comparisons against reference tables."""

from egon_validation.rules.base import SqlRule, RuleResult
from egon_validation.rules.registry import register


@register(
    task="validation-test",
    table="demand.egon_demandregio_cts_ind",
    rule_id="CTS_IND_ROW_COUNT_MATCH",
    scenario_col="scenario",
    economic_sector_col="wz",
    reference_dataset="boundaries.vg250_krs",
    reference_filter="gf = 4",
)
class RowCountComparisonValidation(SqlRule):
    """Validates that grouped row counts match a reference table count.

    This is a custom rule for validating that when a table is grouped by
    certain columns (e.g., scenario, economic_sector), each group has the
    same row count as a reference table.

    Args:
        rule_id: Unique identifier
        table: Table to validate (the table being checked)
        task: Task identifier (optional, set by run_validations)
        reference_dataset: Reference table to compare against (passed in params)
        reference_filter: SQL WHERE clause for reference table (passed in params, default: "TRUE")
        scenario_col: Column name for scenario grouping (passed in params)
        economic_sector_col: Column name for economic sector grouping (passed in params)

    Example:
        >>> validation = RowCountComparisonValidation(
        ...     rule_id="CTS_IND_ROW_COUNT_MATCH",
        ...     table="demand.egon_demandregio_cts_ind",
        ...     reference_dataset="boundaries.vg250_krs",
        ...     reference_filter="gf = 4",
        ...     scenario_col="scenario",
        ...     economic_sector_col="wz"
        ... )
    """

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
            FROM {self.table}
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
            table=self.table,
            success=ok,
            observed=float(mismatching_groups),
            expected=0.0,
            message=message,
            schema=self.schema,
            table_name=self.table_name,
            kind=self.kind,
        )