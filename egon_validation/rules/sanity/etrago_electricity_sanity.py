from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

@register(task="adhoc", dataset="grid.egon_etrago_load", rule_id="ETRAGO_LOAD_SANITY",
          kind="sanity", scenario_col="scn_name")
class EtragoElectricitySanity(SqlRule):
    """
    Sanity check for eTraGo electricity load data.
    
    Validates that load values are within reasonable bounds and
    that there are no unexpected zero or negative values.
    """
    def sql(self, ctx):
        scenario_col = self.params.get("scenario_col", "scn_name")
        
        where_clause = ""
        if ctx.scenario and scenario_col:
            where_clause = f"WHERE {scenario_col} = :scenario"
        
        return f"""
        SELECT 
            COUNT(*) as total_loads,
            COUNT(CASE WHEN p_set < 0 THEN 1 END) as negative_loads,
            COUNT(CASE WHEN p_set = 0 THEN 1 END) as zero_loads,
            COUNT(CASE WHEN p_set > 10000 THEN 1 END) as excessive_loads,
            AVG(p_set) as avg_load,
            MIN(p_set) as min_load,
            MAX(p_set) as max_load
        FROM {self.dataset}
        {where_clause}
        """

    def postprocess(self, row, ctx):
        total_loads = int(row.get("total_loads", 0))
        negative_loads = int(row.get("negative_loads", 0))
        zero_loads = int(row.get("zero_loads", 0))
        excessive_loads = int(row.get("excessive_loads", 0))
        avg_load = float(row.get("avg_load", 0))
        min_load = float(row.get("min_load", 0))
        max_load = float(row.get("max_load", 0))
        
        issues = []
        if negative_loads > 0:
            issues.append(f"{negative_loads} negative loads")
        if excessive_loads > 0:
            issues.append(f"{excessive_loads} loads > 10GW")
            
        ok = len(issues) == 0
        
        if ok:
            message = f"eTraGo loads appear reasonable: {total_loads} loads, avg={avg_load:.1f}MW, range=[{min_load:.1f}, {max_load:.1f}]MW"
        else:
            message = f"eTraGo load issues found: {'; '.join(issues)}"
            
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=len(issues), expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )