from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

@register(task="adhoc", dataset="society.egon_destatis_zensus_household_per_ha_refined", rule_id="RESIDENTIAL_ELECTRICITY_HH_REFINEMENT",
          kind="sanity", rtol=1e-5)
class ResidentialElectricityHhRefinement(SqlRule):
    """
    Sanity check for residential electricity household refinement.
    
    Check sum of aggregated household types after refinement method
    was applied and compare it to the original census values.
    Matches residential_electricity_hh_refinement() from eGon-data.
    """
    def sql(self, ctx):
        return f"""
        WITH refined AS (
            SELECT nuts3, characteristics_code, SUM(hh_10types) as sum_refined
            FROM {self.dataset}
            GROUP BY nuts3, characteristics_code
        ),
        census AS (
            SELECT t.nuts3, t.characteristics_code, sum(orig) as sum_census
            FROM(
                SELECT nuts3, cell_id, characteristics_code,
                        sum(DISTINCT(hh_5types))as orig
                FROM {self.dataset}
                GROUP BY cell_id, characteristics_code, nuts3
            ) AS t
            GROUP BY t.nuts3, t.characteristics_code    
        )
        SELECT 
            COUNT(*) as total_pairs,
            COUNT(CASE WHEN ABS((refined.sum_refined - census.sum_census) / NULLIF(census.sum_census, 0)) > {self.params.get('rtol', 1e-5)} THEN 1 END) as mismatched_pairs,
            AVG(ABS((refined.sum_refined - census.sum_census) / NULLIF(census.sum_census, 0))) as avg_relative_error,
            MAX(ABS((refined.sum_refined - census.sum_census) / NULLIF(census.sum_census, 0))) as max_relative_error,
            SUM(refined.sum_refined) as total_refined,
            SUM(census.sum_census) as total_census,
            COUNT(DISTINCT refined.nuts3) as unique_nuts3,
            COUNT(DISTINCT refined.characteristics_code) as unique_characteristics
        FROM refined
        JOIN census ON refined.nuts3 = census.nuts3 
        AND refined.characteristics_code = census.characteristics_code
        """

    def postprocess(self, row, ctx):
        total_pairs = int(row.get("total_pairs", 0))
        mismatched_pairs = int(row.get("mismatched_pairs", 0))
        max_rel_error = float(row.get("max_relative_error", 0)) if row.get("max_relative_error") else 0
        total_refined = int(row.get("total_refined", 0))
        total_census = int(row.get("total_census", 0))
        unique_nuts3 = int(row.get("unique_nuts3", 0))
        unique_char = int(row.get("unique_characteristics", 0))
        rtol = float(self.params.get("rtol", 1e-5))
        
        ok = (mismatched_pairs == 0)
        
        if ok:
            message = f"All aggregated household types match at NUTS-3: {total_pairs} pairs across {unique_nuts3} NUTS3 regions and {unique_char} household types (refined={total_refined:,}, census={total_census:,})"
        else:
            message = f"{mismatched_pairs}/{total_pairs} household type pairs exceed tolerance {rtol} (max error: {max_rel_error:.6f})"
            
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=mismatched_pairs, expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )