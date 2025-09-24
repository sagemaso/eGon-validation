from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register, register_map


class ReferentialIntegrityValidation(SqlRule):
    """Validates referential integrity between tables without foreign key constraints."""

    def sql(self, ctx):
        foreign_col = self.params.get("foreign_column", "id")
        reference_dataset = self.params.get("reference_dataset")
        reference_col = self.params.get("reference_column", "id")

        base_query = f"""
        SELECT
            COUNT(*) FILTER (WHERE child.{foreign_col} IS NOT NULL) AS total_non_null_references,
            COUNT(*) FILTER (WHERE child.{foreign_col} IS NOT NULL AND parent.{reference_col} IS NOT NULL) AS valid_references,
            COUNT(*) FILTER (WHERE child.{foreign_col} IS NOT NULL AND parent.{reference_col} IS NULL) AS orphaned_references
        FROM
            {self.dataset} as child
        LEFT JOIN
            {reference_dataset} as parent
        ON child.{foreign_col} = parent.{reference_col}
        """

        return base_query

    def postprocess(self, row, ctx):
        total_non_null_references = int(row.get("total_non_null_references") or 0)
        valid_references = int(row.get("valid_references") or 0)
        orphaned_references = int(row.get("orphaned_references") or 0)

        ok = orphaned_references == 0

        foreign_col = self.params.get("foreign_column", "id")
        reference_dataset = self.params.get("reference_dataset")
        reference_col = self.params.get("reference_column", "id")

        if ok:
            message = f"All {total_non_null_references} references in {foreign_col} have valid matches in {reference_dataset}.{reference_col}"
        else:
            message = f"{orphaned_references} orphaned references found in {foreign_col} (out of {total_non_null_references} total non-null references)"

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            dataset=self.dataset,
            success=ok,
            observed=float(orphaned_references),
            expected=0.0,
            message=message,
            severity=Severity.WARNING,
            schema=self.schema,
            table=self.table,
            column=foreign_col,
        )


# Register multiple referential integrity checks
register_map(
    task="validation-test",
    rule_cls=ReferentialIntegrityValidation,
    rule_id="REFERENTIAL_INTEGRITY_CHECK",
    kind="formal",
    datasets_params={
        "grid.egon_etrago_load_timeseries": {
            "foreign_column": "load_id",
            "reference_dataset": "grid.egon_etrago_load",
            "reference_column": "load_id",
        },
        "grid.egon_etrago_load": {
            "foreign_column": "bus",
            "reference_dataset": "grid.egon_etrago_bus",
            "reference_column": "bus_id",
        },
    },
)
