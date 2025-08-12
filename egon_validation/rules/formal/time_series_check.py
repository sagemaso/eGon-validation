from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register, register_map


#@register(task="adhoc", dataset="grid.egon_etrago_load_timeseries", rule_id="TS_LENGTH_CHECK",
#          kind="formal", column="p_set", expected_length=8760)
class TimeSeriesLengthValidation(SqlRule):
    def sql(self, ctx):
        col = self.params.get("column", "values")
        expected_length = self.params.get("expected_length", 8760)
        scenario_col = self.params.get("scenario_col")
        
        base_query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(CASE WHEN cardinality({col}) = {expected_length} THEN 1 END) as correct_length,
            COUNT(CASE WHEN cardinality({col}) != {expected_length} THEN 1 END) as wrong_length,
            array_agg(DISTINCT cardinality({col})) as found_lengths
        FROM {self.dataset}
        """
        
        if ctx.scenario and scenario_col:
            base_query += f" WHERE {scenario_col} = :scenario"
            
        return base_query

    def postprocess(self, row, ctx):
        total_rows = int(row.get("total_rows") or 0)
        correct_length = int(row.get("correct_length") or 0) 
        wrong_length = int(row.get("wrong_length") or 0)
        found_lengths = row.get("found_lengths", [])
        expected_length = self.params.get("expected_length", 8760)
        
        ok = (wrong_length == 0)
        
        if ok:
            message = f"All {total_rows} time series have correct length of {expected_length} ({correct_length} validated)"
        else:
            message = f"{wrong_length} time series with invalid length. Expected: {expected_length}, Found: {found_lengths}"
        
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=wrong_length, expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table, column=self.params.get("column")
        )

register_map(
    task="adhoc",
    rule_cls=TimeSeriesLengthValidation,
    rule_id="TS_LENGTH_CHECK",
    kind="formal",
    datasets_params={
        "demand.egon_demandregio_sites_ind_electricity_dsm_timeseries": {
            "column": "p_set", "expected_length": 8760
        },
        "grid.egon_etrago_load_timeseries": {
            "column": "p_set", "expected_length": 8760
        },
        # weitere Tabellen hier ...
    }
)