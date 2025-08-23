from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register
from egon_validation.config import RESIDENTIAL_ELECTRICITY_RTOL

@register(task="sanity", dataset="demand.egon_demandregio_hh", rule_id="RESIDENTIAL_ELECTRICITY_ANNUAL_SUM",
          kind="sanity", rtol=1e-5, scenario_col="scenario")
class ResidentialElectricitySum(SqlRule):
    """
    Sanity check for residential electricity annual sum.
    
    Aggregate the annual demand of all census cells at NUTS3 to compare
    with initial scaling parameters from DemandRegio.
    Matches residential_electricity_annual_sum() from eGon-data.
    """
    def sql(self, ctx):
        scenario_col = self.params.get("scenario_col", "scenario")
        
        where_clause = ""
        scenario_filter = ""
        if scenario and scenario_col:
            where_clause = f"AND egon.{scenario_col} = :scenario"
            scenario_filter = f"AND profiles.scenario = :scenario"
        
        return f"""
        WITH profiles AS (
            SELECT scenario, SUM(demand) AS profile_sum, vg250_nuts3
            FROM demand.egon_demandregio_zensus_electricity AS egon,
                 boundaries.egon_map_zensus_vg250 AS boundaries
            Where egon.zensus_population_id = boundaries.zensus_population_id
            AND sector = 'residential'
            {where_clause.replace('egon.', 'egon.')}
            GROUP BY vg250_nuts3, scenario
        ),
        dr AS (
            SELECT nuts3, scenario, sum(demand) AS demand_regio_sum
            FROM {self.dataset}
            WHERE 1=1 {scenario_filter.replace('profiles.', 'dr.')}
            GROUP BY year, scenario, nuts3
        )
        SELECT 
            COUNT(*) as total_nuts3_pairs,
            COUNT(CASE WHEN ABS((profiles.profile_sum - dr.demand_regio_sum) / dr.demand_regio_sum) > {self.params.get('rtol', RESIDENTIAL_ELECTRICITY_RTOL)} THEN 1 END) as mismatched_pairs,
            AVG(ABS((profiles.profile_sum - dr.demand_regio_sum) / dr.demand_regio_sum)) as avg_relative_error,
            MAX(ABS((profiles.profile_sum - dr.demand_regio_sum) / dr.demand_regio_sum)) as max_relative_error,
            SUM(profiles.profile_sum) as total_profile_sum,
            SUM(dr.demand_regio_sum) as total_demand_regio_sum
        FROM profiles
        JOIN dr ON profiles.vg250_nuts3 = dr.nuts3 and profiles.scenario = dr.scenario
        """

    def postprocess(self, row, ctx):
        total_pairs = int(row.get("total_nuts3_pairs", 0))
        mismatched_pairs = int(row.get("mismatched_pairs", 0))
        max_rel_error = float(row.get("max_relative_error", 0)) if row.get("max_relative_error") else 0
        total_profile = float(row.get("total_profile_sum", 0))
        total_regio = float(row.get("total_demand_regio_sum", 0))
        rtol = float(self.params.get("rtol", 1e-5))
        
        ok = (mismatched_pairs == 0)
        
        if ok:
            message = f"Aggregated annual residential electricity demand matches DemandRegio at NUTS-3 ({total_pairs} pairs within {rtol} tolerance)"
        else:
            message = f"{mismatched_pairs}/{total_pairs} NUTS3 pairs exceed tolerance {rtol} (max error: {max_rel_error:.6f})"
            
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=mismatched_pairs, expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )