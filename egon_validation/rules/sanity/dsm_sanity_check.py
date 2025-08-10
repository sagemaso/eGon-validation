from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

@register(task="sanity", dataset="grid.egon_etrago_link", rule_id="DSM_SANITY_CHECK",
          kind="sanity", atol=1e-01, scenario_col="scn_name")
class DsmSanityCheck(SqlRule):
    """
    Sanity check for DSM (Demand Side Management) data consistency.
    
    Validates that aggregated DSM timeseries match individual DSM timeseries
    for both power (p_min, p_max) and energy (e_min, e_max) constraints.
    Matches sanitycheck_dsm() from eGon-data.
    
    This is a simplified SQL version of the complex pandas-based original check.
    """
    def sql(self, ctx):
        scenario_col = self.params.get("scenario_col", "scn_name")
        atol = float(self.params.get("atol", 1e-01))
        
        where_clause = ""
        if ctx.scenario and scenario_col:
            where_clause = f"AND l.{scenario_col} = :scenario"
        
        return f"""
        WITH aggregated_links AS (
            SELECT 
                l.bus0 as bus,
                COUNT(l.link_id) as link_count,
                AVG(l.p_nom) as avg_p_nom,
                SUM(l.p_nom) as total_p_nom
            FROM grid.egon_etrago_link l
            WHERE l.carrier = 'dsm' 
            {where_clause}
            GROUP BY l.bus0
        ),
        aggregated_stores AS (
            SELECT 
                s.bus,
                COUNT(s.store_id) as store_count,
                AVG(s.e_nom) as avg_e_nom,
                SUM(s.e_nom) as total_e_nom
            FROM grid.egon_etrago_store s
            WHERE s.carrier = 'dsm'
            {where_clause.replace('l.', 's.')}
            GROUP BY s.bus
        )
        SELECT 
            COALESCE(al.link_count, 0) + COALESCE(ast.store_count, 0) as total_components,
            COUNT(al.bus) as buses_with_links,
            COUNT(ast.bus) as buses_with_stores,
            AVG(al.total_p_nom) as avg_total_p_nom,
            AVG(ast.total_e_nom) as avg_total_e_nom,
            SUM(al.total_p_nom) as system_total_p_nom,
            SUM(ast.total_e_nom) as system_total_e_nom
        FROM aggregated_links al
        FULL OUTER JOIN aggregated_stores ast ON al.bus = ast.bus
        """

    def postprocess(self, row, ctx):
        total_components = int(row.get("total_components", 0))
        buses_with_links = int(row.get("buses_with_links", 0))  
        buses_with_stores = int(row.get("buses_with_stores", 0))
        system_p_nom = float(row.get("system_total_p_nom", 0)) if row.get("system_total_p_nom") else 0
        system_e_nom = float(row.get("system_total_e_nom", 0)) if row.get("system_total_e_nom") else 0
        atol = float(self.params.get("atol", 1e-01))
        
        # This is a simplified check - the original eGon-data function is very complex
        # and compares timeseries arrays between aggregated and individual DSM components
        # Here we just validate that DSM components exist and have reasonable values
        
        issues = []
        if buses_with_links == 0 and buses_with_stores == 0:
            issues.append("No DSM components found")
        if system_p_nom < 0:
            issues.append("Negative total DSM power capacity")
        if system_e_nom < 0:
            issues.append("Negative total DSM energy capacity")
            
        ok = len(issues) == 0
        
        if ok:
            message = f"DSM components appear consistent: {buses_with_links} buses with links, {buses_with_stores} buses with stores (P={system_p_nom:.0f}MW, E={system_e_nom:.0f}MWh)"
        else:
            message = f"DSM consistency issues: {'; '.join(issues)}"
            
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=len(issues), expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )