from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

@register(task="sanity", dataset="supply.egon_home_batteries", rule_id="HOME_BATTERIES_SANITY",
          kind="sanity", scenario_col="scenario")
class HomeBatteriesSanity(SqlRule):
    """
    Sanity check for home battery storage data.
    
    Validates that home battery capacities and power ratings
    are within reasonable bounds for residential applications.
    """
    def sql(self, ctx):
        scenario_col = self.params.get("scenario_col", "scenario")
        
        where_clause = ""
        if scenario and scenario_col:
            where_clause = f"WHERE hb.{scenario_col} = :scenario"
        else:
            where_clause = "WHERE 1=1"
        
        return f"""
        WITH home_battery_data AS (
            SELECT 
                hb.*,
                -- Get cbat_pbat_ratio from etrago storage (replicates get_cbat_pbat_ratio())
                COALESCE((
                    SELECT max_hours::numeric 
                    FROM supply.egon_etrago_storage 
                    WHERE carrier = 'home_battery' 
                    LIMIT 1
                ), 4.0) as cbat_pbat_ratio,
                -- Calculate actual capacity as p_nom * cbat_pbat_ratio (replicates line 1413)
                hb.p_nom * COALESCE((
                    SELECT max_hours::numeric 
                    FROM supply.egon_etrago_storage 
                    WHERE carrier = 'home_battery' 
                    LIMIT 1
                ), 4.0) as calculated_capacity
            FROM {self.dataset} hb
            {where_clause}
        )
        SELECT 
            COUNT(*) as total_batteries,
            COUNT(CASE WHEN p_nom < 0 THEN 1 END) as negative_power,
            COUNT(CASE WHEN p_nom > 50 THEN 1 END) as excessive_power,  -- >50kW unusual for home
            COUNT(CASE WHEN calculated_capacity < 0 THEN 1 END) as negative_energy,
            COUNT(CASE WHEN calculated_capacity > 200 THEN 1 END) as excessive_energy,  -- >200kWh unusual for home
            COUNT(CASE WHEN calculated_capacity > 0 AND p_nom > 0 AND calculated_capacity/p_nom > 20 THEN 1 END) as unusual_duration,
            AVG(p_nom) as avg_power,
            AVG(calculated_capacity) as avg_energy,
            AVG(cbat_pbat_ratio) as avg_ratio,
            MIN(p_nom) as min_power,
            MAX(p_nom) as max_power,
            MIN(calculated_capacity) as min_energy,
            MAX(calculated_capacity) as max_energy,
            COUNT(DISTINCT bus_id) as unique_buses
        FROM home_battery_data
        """

    def postprocess(self, row, ctx):
        total_batteries = int(row.get("total_batteries", 0))
        negative_power = int(row.get("negative_power", 0))
        excessive_power = int(row.get("excessive_power", 0))
        negative_energy = int(row.get("negative_energy", 0))
        excessive_energy = int(row.get("excessive_energy", 0))
        unusual_duration = int(row.get("unusual_duration", 0))
        avg_power = float(row.get("avg_power", 0))
        avg_energy = float(row.get("avg_energy", 0))
        max_power = float(row.get("max_power", 0))
        max_energy = float(row.get("max_energy", 0))
        unique_buses = int(row.get("unique_buses", 0))
        
        issues = []
        if negative_power > 0:
            issues.append(f"{negative_power} negative power ratings")
        if negative_energy > 0:
            issues.append(f"{negative_energy} negative energy capacities")
        if excessive_power > 0:
            issues.append(f"{excessive_power} batteries > 50kW (unusual for homes)")
        if excessive_energy > 0:
            issues.append(f"{excessive_energy} batteries > 200kWh (unusual for homes)")
        if unusual_duration > 0:
            issues.append(f"{unusual_duration} batteries with >20h duration (unusual)")
            
        ok = len(issues) == 0
        
        if ok:
            message = f"Home batteries reasonable: {total_batteries} batteries at {unique_buses} buses, avg={avg_power:.1f}kW/{avg_energy:.1f}kWh"
        else:
            message = f"Home battery issues: {'; '.join(issues)}"
            
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=len(issues), expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )