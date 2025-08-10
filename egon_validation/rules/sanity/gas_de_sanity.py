from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

@register(task="sanity", dataset="supply.egon_gas_production", rule_id="GAS_DE_SANITY",
          kind="sanity", scenario_col="scenario")
class GasDeSanity(SqlRule):
    """
    Sanity check for domestic gas production data in Germany.
    
    Validates that German gas production values are reasonable 
    and within expected bounds for domestic production capacity.
    """
    def sql(self, ctx):
        scenario_col = self.params.get("scenario_col", "scenario")
        
        where_clause = ""
        if ctx.scenario and scenario_col:
            where_clause = f"WHERE {scenario_col} = :scenario"
        
        return f"""
        SELECT 
            COUNT(*) as total_production_sites,
            COUNT(CASE WHEN p_nom < 0 THEN 1 END) as negative_production,
            COUNT(CASE WHEN p_nom = 0 THEN 1 END) as zero_production,
            COUNT(CASE WHEN p_nom > 5000 THEN 1 END) as excessive_production,
            AVG(p_nom) as avg_production,
            MIN(p_nom) as min_production,
            MAX(p_nom) as max_production,
            SUM(p_nom) as total_production,
            COUNT(DISTINCT bus) as unique_buses,
            COUNT(DISTINCT carrier) as unique_carriers
        FROM {self.dataset}
        {where_clause}
        """

    def postprocess(self, row, ctx):
        total_sites = int(row.get("total_production_sites", 0))
        negative_production = int(row.get("negative_production", 0))
        zero_production = int(row.get("zero_production", 0))
        excessive_production = int(row.get("excessive_production", 0))
        avg_production = float(row.get("avg_production", 0))
        total_production = float(row.get("total_production", 0))
        unique_buses = int(row.get("unique_buses", 0))
        unique_carriers = int(row.get("unique_carriers", 0))
        
        issues = []
        if negative_production > 0:
            issues.append(f"{negative_production} negative production values")
        if excessive_production > 0:
            issues.append(f"{excessive_production} sites > 5 GW (unusually high)")
        if total_sites > 0 and zero_production / total_sites > 0.8:
            issues.append(f"Too many zero production sites: {zero_production}/{total_sites}")
            
        ok = len(issues) == 0
        
        if ok:
            message = f"German gas production reasonable: {total_sites} sites at {unique_buses} buses, total={total_production:.0f}MW, {unique_carriers} carriers"
        else:
            message = f"German gas production issues: {'; '.join(issues)}"
            
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=len(issues), expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )