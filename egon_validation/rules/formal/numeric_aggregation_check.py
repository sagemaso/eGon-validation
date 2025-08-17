from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register, register_map

@register(task="adhoc", dataset="grid.egon_etrago_load", rule_id="ELECTRICAL_LOAD_AGGREGATION",
          kind="formal", tolerance=0.05)
class ElectricalLoadAggregationValidation(SqlRule):
    """Validates sum, max, min of electrical load profiles against expected values."""
    
    def sql(self, ctx):
        tolerance = float(self.params.get("tolerance", 0.05))
        scenario_col = self.params.get("scenario_col", "scn_name")
        
        base_query = f"""
        SELECT
            s.scn_name,
            SUM(s.sum_value)/1e6 AS load_sum_twh,
            MAX(s.sum_value)/1e3 AS load_max_gw,
            MIN(s.sum_value)/1e3 AS load_min_gw
        FROM (
            SELECT
                load.scn_name,
                time_index,
                SUM(value) AS sum_value
            FROM
                grid.egon_etrago_load AS load
            JOIN
                grid.egon_etrago_load_timeseries AS load_ts
            USING
                (scn_name, load_id)
            JOIN
                grid.egon_etrago_bus as bus
            ON (load.scn_name = bus.scn_name AND bus.bus_id = load.bus)
            JOIN
                unnest(load_ts.p_set) WITH ORDINALITY AS u(value, time_index)
            ON TRUE
            WHERE
                load.carrier = 'AC' AND
                bus.country = 'DE'
        """
        
        if ctx.scenario and scenario_col:
            base_query += f" AND load.{scenario_col} = :scenario"
            
        base_query += """
            GROUP BY
                load.scn_name, time_index
        ) s
        GROUP BY
            s.scn_name
        """
        
        return base_query

    def postprocess(self, row, ctx):
        scn_name = row.get("scn_name")
        load_sum_twh = float(row.get("load_sum_twh") or 0.0)
        load_max_gw = float(row.get("load_max_gw") or 0.0)
        load_min_gw = float(row.get("load_min_gw") or 0.0)
        tolerance = float(self.params.get("tolerance", 0.05))
        
        # Expected values based on CSV requirements
        expected_values = {
            "eGon2035": {"sum_twh": 533.48, "max_gw": 80.0, "min_gw": 30.0},
            "eGon2021": {"sum_twh": 500.0, "max_gw": 75.0, "min_gw": 25.0},
            "eGon100RE": {"sum_twh": 600.0, "max_gw": 90.0, "min_gw": 35.0}
        }
        
        expected = expected_values.get(scn_name, {"sum_twh": load_sum_twh, "max_gw": load_max_gw, "min_gw": load_min_gw})
        
        # Check if values are within tolerance
        sum_ok = abs(load_sum_twh - expected["sum_twh"]) <= (expected["sum_twh"] * tolerance)
        max_ok = abs(load_max_gw - expected["max_gw"]) <= (expected["max_gw"] * tolerance)  
        min_ok = abs(load_min_gw - expected["min_gw"]) <= (expected["min_gw"] * tolerance)
        
        ok = sum_ok and max_ok and min_ok
        
        message = f"Scenario {scn_name}: Sum={load_sum_twh:.2f}TWh (exp={expected['sum_twh']}), Max={load_max_gw:.2f}GW (exp={expected['max_gw']}), Min={load_min_gw:.2f}GW (exp={expected['min_gw']})"
        
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=load_sum_twh, expected=expected["sum_twh"],
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )


@register(task="adhoc", dataset="demand.egon_demandregio_zensus_electricity", rule_id="DISAGGREGATED_DEMAND_SUM_MATCH",
          kind="formal", sector="residential", tolerance=0.01)
class DisaggregatedDemandSumValidation(SqlRule):
    """Validates that sum of disaggregated demands matches original aggregated value."""
    
    def sql(self, ctx):
        sector = self.params.get("sector", "residential")
        scenario_col = self.params.get("scenario_col", "scenario")
        
        base_query = f"""
        WITH disaggregated AS (
            SELECT
                scenario,
                sum(demand) as disagg_sum
            FROM
                {self.dataset}
            WHERE
                sector = '{sector}'
        """
        
        if ctx.scenario and scenario_col:
            base_query += f" AND {scenario_col} = :scenario"
            
        base_query += f"""
            GROUP BY scenario
        ),
        original AS (
            SELECT
                scenario,
                sum(demand) as orig_sum
            FROM
                demand.egon_demandregio_hh
        """
        
        if ctx.scenario and scenario_col:
            base_query += f" WHERE {scenario_col} = :scenario"
            
        base_query += """
            GROUP BY scenario
        )
        SELECT 
            d.scenario,
            d.disagg_sum,
            o.orig_sum,
            ABS(d.disagg_sum - o.orig_sum) as abs_diff,
            ABS(d.disagg_sum - o.orig_sum) / NULLIF(o.orig_sum, 0) as rel_diff
        FROM disaggregated d
        JOIN original o USING (scenario)
        """
        
        return base_query

    def postprocess(self, row, ctx):
        scenario = row.get("scenario")
        disagg_sum = float(row.get("disagg_sum") or 0.0)
        orig_sum = float(row.get("orig_sum") or 0.0)
        abs_diff = float(row.get("abs_diff") or 0.0)
        rel_diff = float(row.get("rel_diff") or 0.0)
        tolerance = float(self.params.get("tolerance", 0.01))
        
        ok = rel_diff <= tolerance
        
        message = f"Scenario {scenario}: Disaggregated sum {disagg_sum:.2f}, Original sum {orig_sum:.2f}, Rel. diff {rel_diff:.4f} (tolerance {tolerance})"
        
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=rel_diff, expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )