from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register, register_map


class ReferentialIntegrityValidation(SqlRule):
    """Validates referential integrity between tables without foreign key constraints.

    Args:
        rule_id: Unique identifier
        task: Task identifier
        table: Table containing the foreign key (child table)
        fk_column: Foreign key column name in child table (passed in params)
        ref_table: Referenced parent table (passed in params)
        ref_column: Primary/unique key column in parent table (passed in params)

    Example:
        >>> validation = ReferentialIntegrityValidation(
        ...     rule_id="FK_TS_SCENARIO",
        ...     task="validation-test",
        ...     table="facts.timeseries",
        ...     fk_column="scenario_id",
        ...     ref_table="dim.scenarios",
        ...     ref_column="scenario_id"
        ... )
    """

    def sql(self, ctx):
        foreign_col = self.params.get("fk_column", "id")
        ref_table = self.params.get("ref_table")
        reference_col = self.params.get("ref_column", "id")

        base_query = f"""
        SELECT
            COUNT(*) FILTER (WHERE child.{foreign_col} IS NOT NULL) AS total_non_null_references,
            COUNT(*) FILTER (WHERE child.{foreign_col} IS NOT NULL AND parent.{reference_col} IS NOT NULL) AS valid_references,
            COUNT(*) FILTER (WHERE child.{foreign_col} IS NOT NULL AND parent.{reference_col} IS NULL) AS orphaned_references
        FROM
            {self.table} as child
        LEFT JOIN
            {ref_table} as parent
        ON child.{foreign_col} = parent.{reference_col}
        """

        return base_query

    def postprocess(self, row, ctx):
        total_non_null_references = int(row.get("total_non_null_references") or 0)
        valid_references = int(row.get("valid_references") or 0)
        orphaned_references = int(row.get("orphaned_references") or 0)

        ok = orphaned_references == 0

        foreign_col = self.params.get("fk_column", "id")
        ref_table = self.params.get("ref_table")
        reference_col = self.params.get("ref_column", "id")

        if ok:
            message = f"All {total_non_null_references} references in {foreign_col} have valid matches in {ref_table}.{reference_col}"
        else:
            message = f"{orphaned_references} orphaned references found in {foreign_col} (out of {total_non_null_references} total non-null references)"

        return self.create_result(
            success=ok,
            observed=orphaned_references,
            expected=0,
            message=message,
            column=foreign_col,
            severity=Severity.ERROR if not ok else Severity.INFO,
        )
