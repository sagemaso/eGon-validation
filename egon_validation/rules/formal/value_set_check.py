from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register, register_map
import json


@register(
    task="validation-test",
    dataset="demand.egon_demandregio_hh",
    rule_id="SCENARIO_VALUES_VALID",
    kind="formal",
    column="scenario",
    expected_values=["eGon2035", "eGon2021", "eGon100RE"],
)
class ValueSetValidation(SqlRule):
    """Validates that all values in a column are within an expected set of valid values."""

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
        FROM {self.dataset}
        """

        return base_query

    def postprocess(self, row, ctx):
        total_rows = int(row.get("total_rows") or 0)
        valid_values = int(row.get("valid_values") or 0)
        invalid_values = int(row.get("invalid_values") or 0)
        invalid_distinct = row.get("invalid_distinct", [])
        expected_values = self.params.get("expected_values", [])

        ok = invalid_values == 0

        if ok:
            message = f"All {total_rows} values are in expected set {expected_values}"
        else:
            message = f"{invalid_values} invalid values found. Invalid values: {invalid_distinct}"

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            dataset=self.dataset,
            success=ok,
            observed=invalid_values,
            expected=0.0,
            message=message,
            severity=Severity.WARNING,
            schema=self.schema,
            table=self.table,
            column=self.params.get("column"),
        )


# Register multiple rules using register_map for different datasets
register_map(
    task="validation-test",
    rule_cls=ValueSetValidation,
    rule_id="VALUE_SET_VALIDATION",
    kind="formal",
    datasets_params={
        "grid.egon_etrago_bus": {
            "column": "carrier",
            "expected_values": [
                "rural_heat",
                "urban_central_water_tanks",
                "low_voltage",
                "CH4",
                "H2_saltcavern",
                "services_rural_heat",
                "services_rural_water_tanks",
                "central_heat_store",
                "AC",
                "Li_ion",
                "H2_grid",
                "dsm",
                "urban_central_heat",
                "residential_rural_heat",
                "central_heat",
                "rural_heat_store",
                "residential_rural_water_tanks",
            ],
        }
    },
)
