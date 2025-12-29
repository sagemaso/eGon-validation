from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register, register_map
from egon_validation.config import ARRAY_CARDINALITY_ANNUAL_HOURS


@register(
    task="validation-test",
    table="grid.egon_etrago_load_timeseries",
    rule_id="LOAD_TIMESERIES_LENGTH",
    array_column="p_set",
    expected_length=8760,
)
class ArrayCardinalityValidation(SqlRule):
    """Validates that array columns have the expected cardinality (length).

    Args:
        rule_id: Unique identifier
        task: Task identifier
        table: Full table name including schema
        array_column: Name of the array column to validate (passed in params)
        expected_length: Expected array length (cardinality, passed in params)

    Example:
        >>> validation = ArrayCardinalityValidation(
        ...     rule_id="TS_VALUES_LEN_8760",
        ...     task="validation-test",
        ...     table="facts.timeseries",
        ...     array_column="values",
        ...     expected_length=8760
        ... )
    """

    def sql(self, ctx):
        array_col = self.params.get("array_column", "values")
        expected_length = int(
            self.params.get("expected_length", ARRAY_CARDINALITY_ANNUAL_HOURS)
        )

        base_query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(CASE WHEN cardinality({array_col}) = {expected_length} THEN 1 END) as correct_length,
            COUNT(CASE WHEN cardinality({array_col}) != {expected_length} THEN 1 END) as wrong_length,
            COUNT(CASE WHEN {array_col} IS NULL THEN 1 END) as null_arrays,
            array_agg(DISTINCT cardinality({array_col})) as found_lengths,
            MIN(cardinality({array_col})) as min_length,
            MAX(cardinality({array_col})) as max_length,
            AVG(cardinality({array_col})) as avg_length
        FROM {self.table}
        """

        return base_query

    def postprocess(self, row, ctx):
        total_rows = int(row.get("total_rows") or 0)
        correct_length = int(row.get("correct_length") or 0)
        wrong_length = int(row.get("wrong_length") or 0)
        null_arrays = int(row.get("null_arrays") or 0)
        found_lengths = row.get("found_lengths", [])
        min_length = row.get("min_length")
        max_length = row.get("max_length")
        avg_length = row.get("avg_length")
        expected_length = int(
            self.params.get("expected_length", ARRAY_CARDINALITY_ANNUAL_HOURS)
        )

        ok = (wrong_length == 0) and (null_arrays == 0)

        if ok:
            message = (
                f"All {total_rows} arrays have correct length of {expected_length}"
            )
        else:
            problems = []
            if wrong_length > 0:
                problems.append(f"{wrong_length} arrays with wrong length")
            if null_arrays > 0:
                problems.append(f"{null_arrays} NULL arrays")

            details = f"Expected: {expected_length}, Found lengths: {found_lengths}"
            if min_length is not None and max_length is not None:
                details += f", Range: {min_length}-{max_length}"
            if avg_length is not None:
                details += f", Avg: {avg_length:.2f}"

            message = "; ".join(problems) + f" ({details})"

        return self.create_result(
            success=ok,
            observed=float(wrong_length),
            expected=0.0,
            message=message,
            column=self.params.get("array_column"),
        )


# Register array cardinality validation for multiple datasets with timeseries
register_map(
    task="validation-test",
    rule_cls=ArrayCardinalityValidation,
    rule_id="ARRAY_CARDINALITY_CHECK",
    tables_params={
        "demand.egon_demandregio_sites_ind_electricity_dsm_timeseries": {
            "array_column": "p_set",
            "expected_length": 8760,
        },
        "demand.egon_demandregio_timeseries_cts_ind": {
            "array_column": "load_curve",
            "expected_length": 8760,
        },
        "demand.egon_etrago_electricity_cts_dsm_timeseries": {
            "array_column": "p_set",
            "expected_length": 8760,
        },
        "demand.egon_etrago_timeseries_individual_heating": {
            "array_column": "dist_aggregated_mw",
            "expected_length": 8760,
        },
        "demand.egon_heat_timeseries_selected_profiles": {
            "array_column": "selected_idp_profiles",
            "expected_length": 365,
        },
        "demand.egon_osm_ind_load_curves_individual_dsm_timeseries": {
            "array_column": "p_set",
            "expected_length": 8760,
        },
        "demand.egon_sites_ind_load_curves_individual_dsm_timeseries": {
            "array_column": "p_set",
            "expected_length": 8760,
        },
        "demand.egon_timeseries_district_heating": {
            "array_column": "dist_aggregated_mw",
            "expected_length": 8760,
        },
        "grid.egon_etrago_bus_timeseries": {
            "array_column": "v_mag_pu_set",
            "expected_length": 8760,
        },
        "grid.egon_etrago_generator_timeseries": {
            "array_column": "p_max_pu",
            "expected_length": 8760,
        },
        # "grid.egon_etrago_line_timeseries": {
        #     "array_column": "s_max_pu", "expected_length": 8760
        # }, 23239 NULL arrays (Expected: 8760, Found lengths: [8760, None], Range: 8760-8760, Avg: 8760.00)
        # "grid.egon_etrago_link_timeseries": {
        #     "array_column": "p_min_pu", "expected_length": 8760
        # },    23239 NULL arrays (Expected: 8760, Found lengths: [8760, None], Range: 8760-8760, Avg: 8760.00)
        "grid.egon_etrago_load_timeseries": {
            "array_column": "p_set",
            "expected_length": 8760,
        },
        "grid.egon_etrago_storage_timeseries": {
            "array_column": "inflow",
            "expected_length": 8760,
        },
        "grid.egon_etrago_store_timeseries": {
            "array_column": "e_min_pu",
            "expected_length": 8760,
        },
        "grid.egon_etrago_transformer_timeseries": {
            "array_column": "s_max_pu",
            "expected_length": 8760,
        },
    },
)
