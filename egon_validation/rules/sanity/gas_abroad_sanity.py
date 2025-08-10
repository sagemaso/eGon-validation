from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

@register(task="sanity", dataset="supply.egon_gas_abroad_import", rule_id="GAS_ABROAD_SANITY",
          kind="sanity", scenario_col="scenario")
class GasAbroadSanity(SqlRule):
    """
    Sanity check for gas import from abroad data.
    
    Validates that gas import values are reasonable and consistent
    with expected energy flows and capacities.
    """
    def sql(self, ctx):
        scenario_col = self.params.get("scenario_col", "scenario")
        
        where_clause = ""
        if ctx.scenario and scenario_col:
            where_clause = f"WHERE {scenario_col} = :scenario"
        
        return f"""
        SELECT 
            COUNT(*) as total_imports,
            COUNT(CASE WHEN capacity < 0 THEN 1 END) as negative_capacities,
            COUNT(CASE WHEN capacity = 0 THEN 1 END) as zero_capacities,
            COUNT(CASE WHEN capacity > 50000 THEN 1 END) as excessive_capacities,
            AVG(capacity) as avg_capacity,
            MIN(capacity) as min_capacity,
            MAX(capacity) as max_capacity,
            SUM(capacity) as total_capacity,
            COUNT(DISTINCT bus_id) as unique_buses
        FROM {self.dataset}
        {where_clause}
        """

    def postprocess(self, row, ctx):
        total_imports = int(row.get("total_imports", 0))
        negative_capacities = int(row.get("negative_capacities", 0))
        zero_capacities = int(row.get("zero_capacities", 0))
        excessive_capacities = int(row.get("excessive_capacities", 0))
        avg_capacity = float(row.get("avg_capacity", 0))
        total_capacity = float(row.get("total_capacity", 0))
        unique_buses = int(row.get("unique_buses", 0))
        
        issues = []
        if negative_capacities > 0:
            issues.append(f"{negative_capacities} negative capacities")
        if excessive_capacities > 0:
            issues.append(f"{excessive_capacities} capacities > 50 GW")
        if total_imports > 0 and zero_capacities / total_imports > 0.5:
            issues.append(f"Too many zero capacities: {zero_capacities}/{total_imports}")
            
        ok = len(issues) == 0
        
        if ok:
            message = f"Gas abroad imports reasonable: {total_imports} import points at {unique_buses} buses, total={total_capacity:.0f}MW"
        else:
            message = f"Gas abroad import issues: {'; '.join(issues)}"
            
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=len(issues), expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )