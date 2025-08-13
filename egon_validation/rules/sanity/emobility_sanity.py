from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

@register(task="adhoc", dataset="demand.egon_ev_count_mv_grid_district", rule_id="EMOBILITY_SANITY",
          kind="sanity", scenario_col="scenario")
class EmobilitySanity(SqlRule):
    """
    Sanity check for e-mobility EV allocation data.
    
    Validates EV counts and allocation to grid districts, based on
    sanitycheck_emobility_mit() from eGon-data (line 833).
    
    Replicates the EV count validation from lines 868-876 of the original function.
    """
    def sql(self, ctx):
        scenario_col = self.params.get("scenario_col", "scenario")
        
        where_clause = ""
        if ctx.scenario and scenario_col:
            where_clause = f"WHERE {scenario_col} = :scenario"
        
        return f"""
        SELECT 
            COUNT(*) as total_grid_districts,
            SUM(bev_mini + bev_medium + bev_luxury + phev_mini + phev_medium + phev_luxury) as total_ev_count,
            SUM(bev_mini + bev_medium + bev_luxury) as total_bev_count,
            SUM(phev_mini + phev_medium + phev_luxury) as total_phev_count,
            AVG(bev_mini + bev_medium + bev_luxury + phev_mini + phev_medium + phev_luxury) as avg_ev_per_grid,
            COUNT(CASE WHEN bev_mini + bev_medium + bev_luxury + phev_mini + phev_medium + phev_luxury = 0 THEN 1 END) as grids_without_ev,
            COUNT(CASE WHEN bev_mini + bev_medium + bev_luxury + phev_mini + phev_medium + phev_luxury > 1000 THEN 1 END) as grids_with_many_ev,
            COUNT(DISTINCT bus_id) as unique_buses,
            COUNT(DISTINCT scenario_variation) as scenario_variations
        FROM {self.dataset}
        {where_clause}
        """

    def postprocess(self, row, ctx):
        total_grids = int(row.get("total_grid_districts", 0))
        total_ev_count = int(row.get("total_ev_count", 0))
        total_bev = int(row.get("total_bev_count", 0))
        total_phev = int(row.get("total_phev_count", 0))
        avg_ev_per_grid = float(row.get("avg_ev_per_grid", 0))
        grids_without_ev = int(row.get("grids_without_ev", 0))
        grids_with_many_ev = int(row.get("grids_with_many_ev", 0))
        unique_buses = int(row.get("unique_buses", 0))
        scenario_variations = int(row.get("scenario_variations", 0))
        
        issues = []
        if total_ev_count == 0:
            issues.append("No EVs allocated")
        if grids_without_ev > total_grids * 0.5:  # More than 50% grids without EVs is suspicious
            issues.append(f"{grids_without_ev}/{total_grids} grids without EVs ({100*grids_without_ev/total_grids:.1f}%)")
        if grids_with_many_ev > total_grids * 0.1:  # More than 10% grids with >1000 EVs is suspicious  
            issues.append(f"{grids_with_many_ev} grids with >1000 EVs (unusual concentration)")
        if total_bev == 0 and total_phev == 0:
            issues.append("No BEV or PHEV vehicles found")
            
        ok = len(issues) == 0
        
        if ok:
            message = f"E-mobility allocation reasonable: {total_ev_count} EVs ({total_bev} BEV, {total_phev} PHEV) across {unique_buses} buses in {total_grids} grids, avg={avg_ev_per_grid:.1f} EV/grid"
        else:
            message = f"E-mobility allocation issues: {'; '.join(issues)}"
            
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=len(issues), expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )