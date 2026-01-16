from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register, register_map


@register(
    task="validation-test",
    table="demand.egon_demandregio_hh",
    rule_id="SCENARIO_VALUES_VALID",
    column="scenario",
    expected_values=["eGon2035", "eGon2021", "eGon100RE"],
)
class ValueSetValidation(SqlRule):
    """Validates that all values in a column are within an expected set of valid values.

    Args:
        rule_id: Unique identifier
        task: Task identifier
        table: Full table name including schema
        column: Column name to check (passed in params)
        expected_values: List of valid values (passed in params)

    Example:
        >>> validation = ValueSetValidation(
        ...     rule_id="SCENARIO_VALUES_CHECK",
        ...     task="validation-test",
        ...     table="demand.egon_demandregio_hh",
        ...     column="scenario",
        ...     expected_values=["eGon2035", "eGon2021"]
        ... )
    """

    def sql(self, ctx):
        col = self.params.get("column", "value")
        expected_values = self.params.get("expected_values", [])

        # Create SQL array literal for PostgreSQL
        expected_array = "ARRAY[" + ",".join([f"'{v}'" for v in expected_values]) + "]"

        base_query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(CASE WHEN {col} = ANY({expected_array}) THEN 1 END) as valid_values,
            COUNT(CASE WHEN {col} NOT IN (SELECT unnest({expected_array})) OR {col} IS NULL THEN 1 END) as invalid_values,
            array_agg(DISTINCT {col}) FILTER (WHERE {col} NOT IN (SELECT unnest({expected_array})) OR {col} IS NULL) as invalid_distinct
        FROM {self.table}
        """

        return base_query

    def postprocess(self, row, ctx):
        total_rows = int(row.get("total_rows") or 0)
        invalid_values = int(row.get("invalid_values") or 0)
        invalid_distinct = row.get("invalid_distinct", [])
        expected_values = self.params.get("expected_values", [])

        ok = invalid_values == 0

        if ok:
            message = f"All {total_rows} values are in expected set {expected_values}"
        else:
            message = f"{invalid_values} invalid values found. Invalid values: {invalid_distinct}"

        return self.create_result(
            success=ok,
            observed=invalid_values,
            expected=0,
            message=message,
            severity=Severity.ERROR if not ok else Severity.INFO,
        )
