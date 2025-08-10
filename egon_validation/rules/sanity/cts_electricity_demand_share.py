from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

@register(task="sanity", dataset="demand.egon_cts_electricity_demand_building_share", 
          rule_id="CTS_ELECTRICITY_DEMAND_SHARE", kind="sanity", rtol=1e-5, scenario_col="scenario")
class CtsElectricityDemandShare(SqlRule):
    """
    Sanity check for CTS electricity demand share consistency.
    
    Check sum of aggregated cts electricity demand share which equals to one
    for every substation as the substation profile is linearly disaggregated
    to all buildings. Matches cts_electricity_demand_share() from eGon-data.
    """
    def sql(self, ctx):
        # Get all demand shares grouped by bus_id and scenario to check if they sum to 1.0
        scenario_col = self.params.get("scenario_col", "scenario")
        
        where_clause = ""
        if ctx.scenario and scenario_col:
            where_clause = f"WHERE {scenario_col} = :scenario"
        
        return f"""
        WITH grouped_shares AS (
            SELECT 
                bus_id,
                scenario,
                SUM(profile_share) as share_sum,
                COUNT(*) as share_count
            FROM {self.dataset}
            {where_clause}
            GROUP BY bus_id, scenario
        )
        SELECT 
            COUNT(*) as total_bus_scenario_pairs,
            COUNT(CASE WHEN ABS(share_sum - 1.0) > {self.params.get('rtol', 1e-5)} THEN 1 END) as violating_pairs,
            AVG(share_sum) as avg_share_sum,
            MIN(share_sum) as min_share_sum,
            MAX(share_sum) as max_share_sum,
            MAX(ABS(share_sum - 1.0)) as max_deviation
        FROM grouped_shares
        """

    def postprocess(self, row, ctx):
        total_pairs = int(row.get("total_bus_scenario_pairs", 0))
        violating_pairs = int(row.get("violating_pairs", 0))
        max_deviation = float(row.get("max_deviation", 0)) if row.get("max_deviation") else 0
        rtol = float(self.params.get("rtol", 1e-5))
        
        ok = (violating_pairs == 0)
        
        if ok:
            message = f"All {total_pairs} bus/scenario pairs have demand shares summing to 1.0 within tolerance {rtol}"
        else:
            message = f"{violating_pairs}/{total_pairs} bus/scenario pairs violate sum=1.0 (max deviation: {max_deviation:.6f}, tolerance: {rtol})"
            
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=violating_pairs, expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )