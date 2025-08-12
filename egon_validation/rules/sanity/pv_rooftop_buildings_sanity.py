from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

@register(task="adhoc", dataset="supply.egon_pv_rooftop", rule_id="PV_ROOFTOP_SANITY",
          kind="sanity", scenario_col="scenario")
class PvRooftopBuildingsSanity(SqlRule):
    """
    Sanity check for rooftop PV on buildings.
    
    Validates that PV capacity values are reasonable for rooftop installations
    and checks for consistency in the distribution across buildings.
    """
    def sql(self, ctx):
        scenario_col = self.params.get("scenario_col", "scenario")
        
        where_clause = ""
        if ctx.scenario and scenario_col:
            where_clause = f"WHERE {scenario_col} = :scenario"
        
        return f"""
        SELECT 
            COUNT(*) as total_pv_systems,
            COUNT(CASE WHEN p_nom < 0 THEN 1 END) as negative_capacity,
            COUNT(CASE WHEN p_nom = 0 THEN 1 END) as zero_capacity,
            COUNT(CASE WHEN p_nom > 1000 THEN 1 END) as excessive_capacity,  -- >1MW unusual for rooftop
            COUNT(CASE WHEN p_nom BETWEEN 0.001 AND 100 THEN 1 END) as reasonable_capacity,
            AVG(p_nom) as avg_capacity,
            MIN(p_nom) as min_capacity,
            MAX(p_nom) as max_capacity,
            SUM(p_nom) as total_capacity,
            COUNT(DISTINCT bus) as unique_buses,
            STDDEV(p_nom) as stddev_capacity
        FROM {self.dataset}
        {where_clause}
        """

    def postprocess(self, row, ctx):
        total_pv = int(row.get("total_pv_systems", 0))
        negative_capacity = int(row.get("negative_capacity", 0))
        zero_capacity = int(row.get("zero_capacity", 0))
        excessive_capacity = int(row.get("excessive_capacity", 0))
        reasonable_capacity = int(row.get("reasonable_capacity", 0))
        avg_capacity = float(row.get("avg_capacity", 0))
        total_capacity = float(row.get("total_capacity", 0))
        max_capacity = float(row.get("max_capacity", 0))
        unique_buses = int(row.get("unique_buses", 0))
        
        issues = []
        if negative_capacity > 0:
            issues.append(f"{negative_capacity} negative capacities")
        if excessive_capacity > 0:
            issues.append(f"{excessive_capacity} rooftop PV > 1MW (unusual)")
        if total_pv > 0 and zero_capacity / total_pv > 0.1:
            issues.append(f"Many zero capacities: {zero_capacity}/{total_pv} ({100*zero_capacity/total_pv:.1f}%)")
        if total_pv > 0 and reasonable_capacity / total_pv < 0.8:
            issues.append(f"Low fraction of reasonable sizes: {reasonable_capacity}/{total_pv} ({100*reasonable_capacity/total_pv:.1f}%)")
            
        ok = len(issues) == 0
        
        if ok:
            message = f"Rooftop PV reasonable: {total_pv} systems at {unique_buses} buses, total={total_capacity:.0f}MW, avg={avg_capacity:.2f}MW"
        else:
            message = f"Rooftop PV issues: {'; '.join(issues)}"
            
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=len(issues), expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )