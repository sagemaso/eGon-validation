from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

@register(task="adhoc", dataset="demand.egon_emob_mit_load", rule_id="EMOBILITY_SANITY",
          kind="sanity", scenario_col="scenario", min_load=0, max_load=50)
class EmobilitySanity(SqlRule):
    """
    Sanity check for e-mobility load data.
    
    Validates that e-mobility charging loads are within reasonable bounds
    and checks for consistency in the data.
    """
    def sql(self, ctx):
        scenario_col = self.params.get("scenario_col", "scenario")
        min_load = float(self.params.get("min_load", 0))
        max_load = float(self.params.get("max_load", 50))  # 50MW reasonable max for single charging point
        
        where_clause = ""
        if ctx.scenario and scenario_col:
            where_clause = f"WHERE {scenario_col} = :scenario"
        
        return f"""
        SELECT 
            COUNT(*) as total_loads,
            COUNT(CASE WHEN p_set < {min_load} THEN 1 END) as negative_loads,
            COUNT(CASE WHEN p_set = 0 THEN 1 END) as zero_loads,
            COUNT(CASE WHEN p_set > {max_load} THEN 1 END) as excessive_loads,
            AVG(p_set) as avg_load,
            MIN(p_set) as min_load,
            MAX(p_set) as max_load,
            COUNT(DISTINCT bus) as unique_buses
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
        unique_buses = int(row.get("unique_buses", 0))
        
        issues = []
        if negative_loads > 0:
            issues.append(f"{negative_loads} negative loads")
        if excessive_loads > 0:
            issues.append(f"{excessive_loads} loads > {self.params.get('max_load', 50)}MW")
        if total_loads > 0 and zero_loads / total_loads > 0.9:  # More than 90% zeros suspicious
            issues.append(f"{zero_loads}/{total_loads} loads are zero ({100*zero_loads/total_loads:.1f}%)")
            
        ok = len(issues) == 0
        
        if ok:
            message = f"E-mobility loads reasonable: {total_loads} loads across {unique_buses} buses, avg={avg_load:.2f}MW"
        else:
            message = f"E-mobility issues: {'; '.join(issues)}"
            
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=len(issues), expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )