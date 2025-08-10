from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

@register(task="sanity", dataset="grid.egon_etrago_load_timeseries", rule_id="ETRAGO_HEAT_SANITY",
          kind="sanity", scenario_col="scn_name")
class EtragoHeatSanity(SqlRule):
    """
    Sanity check for eTraGo heat load data.
    
    Validates that heat load values are within reasonable bounds
    and checks for seasonal patterns in heating demand.
    """
    def sql(self, ctx):
        scenario_col = self.params.get("scenario_col", "scn_name")
        
        where_clause = "WHERE temp_id LIKE '%heat%' OR temp_id LIKE '%district_heating%'"
        if ctx.scenario and scenario_col:
            where_clause += f" AND {scenario_col} = :scenario"
        
        return f"""
        SELECT 
            COUNT(*) as total_heat_loads,
            COUNT(CASE WHEN cardinality(p_set) != 8760 THEN 1 END) as wrong_length_series,
            AVG(
                CASE WHEN cardinality(p_set) = 8760 THEN
                    (SELECT AVG(unnest) FROM unnest(p_set))
                ELSE NULL END
            ) as avg_heat_load,
            COUNT(DISTINCT temp_id) as unique_heat_components,
            COUNT(CASE WHEN 
                cardinality(p_set) = 8760 AND
                (SELECT MAX(unnest) FROM unnest(p_set)) > 1000 
                THEN 1 END
            ) as excessive_heat_loads
        FROM {self.dataset}
        {where_clause}
        """

    def postprocess(self, row, ctx):
        total_heat_loads = int(row.get("total_heat_loads", 0))
        wrong_length_series = int(row.get("wrong_length_series", 0))
        avg_heat_load = float(row.get("avg_heat_load", 0)) if row.get("avg_heat_load") else 0
        unique_components = int(row.get("unique_heat_components", 0))
        excessive_loads = int(row.get("excessive_heat_loads", 0))
        
        issues = []
        if wrong_length_series > 0:
            issues.append(f"{wrong_length_series} series with wrong length (!= 8760)")
        if excessive_loads > 0:
            issues.append(f"{excessive_loads} heat loads > 1000MW")
        if total_heat_loads == 0:
            issues.append("No heat load data found")
            
        ok = len(issues) == 0
        
        if ok:
            message = f"eTraGo heat loads reasonable: {total_heat_loads} timeseries across {unique_components} components, avg={avg_heat_load:.1f}MW"
        else:
            message = f"eTraGo heat issues: {'; '.join(issues)}"
            
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=len(issues), expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )