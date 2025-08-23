from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register, register_map
from egon_validation.config import ELECTRICAL_LOAD_EXPECTED_VALUES, DISAGGREGATED_DEMAND_TOLERANCE

@register(task="adhoc", dataset="grid.egon_etrago_load", rule_id="ELECTRICAL_LOAD_AGGREGATION",
          kind="custom", tolerance=0.05)
class ElectricalLoadAggregationValidation(SqlRule):
    """Validates sum, max, min of electrical load profiles against expected values."""
    
    def sql(self, ctx):
        tolerance = float(self.params.get("tolerance", 0.05))
        
        base_query = f"""
        SELECT
            json_agg(
                json_build_object(
                    'scn_name', agg.scn_name,
                    'load_sum_twh', agg.load_sum_twh,
                    'load_max_gw', agg.load_max_gw,
                    'load_min_gw', agg.load_min_gw
                )
            ) as scenarios_data
        FROM (
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
            
        base_query += """
                GROUP BY
                    load.scn_name, time_index
            ) s
            GROUP BY
                s.scn_name
        ) agg
        """
        
        return base_query

    def postprocess(self, row, ctx):
        import json
        
        scenarios_data_json = row.get("scenarios_data")
        if not scenarios_data_json:
            return RuleResult(
                rule_id=self.rule_id, task=self.task, dataset=self.dataset,
                success=False, message="No scenario data found",
                severity=Severity.ERROR, schema=self.schema, table=self.table
            )
        
        scenarios_data = json.loads(scenarios_data_json) if isinstance(scenarios_data_json, str) else scenarios_data_json
        if not scenarios_data:
            return RuleResult(
                rule_id=self.rule_id, task=self.task, dataset=self.dataset,
                success=False, message="No scenario data found",
                severity=Severity.ERROR, schema=self.schema, table=self.table
            )
        
        tolerance = float(self.params.get("tolerance", 0.05))
        
        # Expected values from config
        expected_values = ELECTRICAL_LOAD_EXPECTED_VALUES
        
        scenario_results = []
        all_scenarios_ok = True
        total_observed = 0.0
        total_expected = 0.0
        
        for scenario_data in scenarios_data:
            scn_name = scenario_data.get("scn_name")
            load_sum_twh = float(scenario_data.get("load_sum_twh") or 0.0)
            load_max_gw = float(scenario_data.get("load_max_gw") or 0.0)
            load_min_gw = float(scenario_data.get("load_min_gw") or 0.0)
            
            if scn_name not in expected_values:
                # Fail validation for scenarios without expected values
                scenario_ok = False
                all_scenarios_ok = False
                total_observed += load_sum_twh
                # Don't add to total_expected since we have no expected value
                scenario_results.append(f"✗ {scn_name}: Sum={load_sum_twh:.2f}TWh, Max={load_max_gw:.2f}GW, Min={load_min_gw:.2f}GW (NO EXPECTED VALUES)")
                continue
            
            expected = expected_values[scn_name]
            
            # Check if values are within tolerance
            sum_ok = abs(load_sum_twh - expected["sum_twh"]) <= (expected["sum_twh"] * tolerance)
            max_ok = abs(load_max_gw - expected["max_gw"]) <= (expected["max_gw"] * tolerance)  
            min_ok = abs(load_min_gw - expected["min_gw"]) <= (expected["min_gw"] * tolerance)
            
            scenario_ok = sum_ok and max_ok and min_ok
            all_scenarios_ok = all_scenarios_ok and scenario_ok
            
            total_observed += load_sum_twh
            total_expected += expected["sum_twh"]
            
            status = "✓" if scenario_ok else "✗"
            scenario_results.append(f"{status} {scn_name}: Sum={load_sum_twh:.2f}TWh (exp={expected['sum_twh']}), Max={load_max_gw:.2f}GW (exp={expected['max_gw']}), Min={load_min_gw:.2f}GW (exp={expected['min_gw']})")
        
        message = "; ".join(scenario_results)
        
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=all_scenarios_ok, observed=total_observed, expected=total_expected,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )


@register(task="adhoc", dataset="demand.egon_demandregio_zensus_electricity", rule_id="DISAGGREGATED_DEMAND_SUM_MATCH",
          kind="formal", sector="residential", tolerance=0.01)
class DisaggregatedDemandSumValidation(SqlRule):
    """Validates that sum of disaggregated demands matches original aggregated value."""
    
    def sql(self, ctx):
        sector = self.params.get("sector", "residential")
        
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
        tolerance = float(self.params.get("tolerance", DISAGGREGATED_DEMAND_TOLERANCE))
        
        ok = rel_diff <= tolerance
        
        message = f"Scenario {scenario}: Disaggregated sum {disagg_sum:.2f}, Original sum {orig_sum:.2f}, Rel. diff {rel_diff:.4f} (tolerance {tolerance})"
        
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=rel_diff, expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )