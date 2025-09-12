import pytest
from unittest.mock import Mock, patch
from egon_validation.rules.custom.numeric_aggregation_check import (
    ElectricalLoadAggregationValidation,
    DisaggregatedDemandSumValidation,
)
from egon_validation.rules.base import RuleResult, Severity


class TestElectricalLoadAggregationValidation:
    def test_sql_generation(self):
        rule = ElectricalLoadAggregationValidation(
            "test_rule", "test_task", "grid.egon_etrago_load", tolerance=0.05
        )
        sql = rule.sql(None)

        assert "SELECT" in sql
        assert "json_agg" in sql
        assert "scn_name" in sql
        assert "load_sum_twh" in sql
        assert "load_max_gw" in sql
        assert "load_min_gw" in sql
        assert "grid.egon_etrago_load" in sql
        assert "carrier = 'AC'" in sql
        assert "country = 'DE'" in sql

    def test_postprocess_no_scenario_data(self):
        rule = ElectricalLoadAggregationValidation(
            "test_rule", "test_task", "grid.egon_etrago_load"
        )
        row = {"scenarios_data": None}

        result = rule.postprocess(row, None)

        assert result.success is False
        assert result.message == "No scenario data found"
        assert result.severity == Severity.ERROR

    def test_postprocess_empty_scenario_data(self):
        rule = ElectricalLoadAggregationValidation(
            "test_rule", "test_task", "grid.egon_etrago_load"
        )
        row = {"scenarios_data": []}

        result = rule.postprocess(row, None)

        assert result.success is False
        assert result.message == "No scenario data found"

    def test_postprocess_scenario_within_tolerance(self):
        # Use actual config values: eGon2035: sum_twh=533.48, max_gw=109.38, min_gw=31.60
        rule = ElectricalLoadAggregationValidation(
            "test_rule", "test_task", "grid.egon_etrago_load", tolerance=0.05
        )

        scenarios_data = [
            {
                "scn_name": "eGon2035",
                "load_sum_twh": 535.0,  # Within 5% of 533.48
                "load_max_gw": 110.0,  # Within 5% of 109.38
                "load_min_gw": 32.0,  # Within 5% of 31.60
            }
        ]
        row = {"scenarios_data": scenarios_data}

        result = rule.postprocess(row, None)

        assert result.success is True
        assert "✓ eGon2035" in result.message
        assert result.observed == 535.0
        assert result.expected == 533.48

    def test_postprocess_scenario_outside_tolerance(self):
        # Use actual config values with values outside tolerance
        rule = ElectricalLoadAggregationValidation(
            "test_rule", "test_task", "grid.egon_etrago_load", tolerance=0.05
        )

        scenarios_data = [
            {
                "scn_name": "eGon2035",
                "load_sum_twh": 600.0,  # Outside 5% of 533.48
                "load_max_gw": 110.0,
                "load_min_gw": 32.0,
            }
        ]
        row = {"scenarios_data": scenarios_data}

        result = rule.postprocess(row, None)

        assert result.success is False
        assert "✗ eGon2035" in result.message

    @patch("egon_validation.config.ELECTRICAL_LOAD_EXPECTED_VALUES", {})
    def test_postprocess_scenario_no_expected_values(self):
        rule = ElectricalLoadAggregationValidation(
            "test_rule", "test_task", "grid.egon_etrago_load"
        )

        scenarios_data = [
            {
                "scn_name": "unknown_scenario",
                "load_sum_twh": 500.0,
                "load_max_gw": 80.0,
                "load_min_gw": 40.0,
            }
        ]
        row = {"scenarios_data": scenarios_data}

        result = rule.postprocess(row, None)

        assert result.success is False
        assert "NO EXPECTED VALUES" in result.message
        assert "unknown_scenario" in result.message

    def test_postprocess_multiple_scenarios(self):
        # Use actual config values: eGon2035: 533.48, eGon100RE: 581.98
        rule = ElectricalLoadAggregationValidation(
            "test_rule", "test_task", "grid.egon_etrago_load", tolerance=0.05
        )

        scenarios_data = [
            {
                "scn_name": "eGon2035",
                "load_sum_twh": 535.0,  # Within 5% of 533.48
                "load_max_gw": 110.0,  # Within 5% of 109.38
                "load_min_gw": 32.0,  # Within 5% of 31.60
            },
            {
                "scn_name": "eGon100RE",
                "load_sum_twh": 580.0,  # Within 5% of 581.98
                "load_max_gw": 105.0,  # Within 5% of 107.44
                "load_min_gw": 41.0,  # Within 5% of 40.15
            },
        ]
        row = {"scenarios_data": scenarios_data}

        result = rule.postprocess(row, None)

        assert result.success is True
        assert "✓ eGon2035" in result.message
        assert "✓ eGon100RE" in result.message
        assert result.observed == 1115.0  # 535 + 580
        assert result.expected == 1115.46  # 533.48 + 581.98

    def test_postprocess_json_string_parsing(self):
        import json

        rule = ElectricalLoadAggregationValidation(
            "test_rule", "test_task", "grid.egon_etrago_load"
        )

        scenarios_data = [
            {
                "scn_name": "test",
                "load_sum_twh": 100,
                "load_max_gw": 10,
                "load_min_gw": 5,
            }
        ]
        row = {"scenarios_data": json.dumps(scenarios_data)}

        with patch("egon_validation.config.ELECTRICAL_LOAD_EXPECTED_VALUES", {}):
            result = rule.postprocess(row, None)

        # Should handle JSON string input
        assert "test" in result.message


class TestDisaggregatedDemandSumValidation:
    def test_sql_generation_default_sector(self):
        rule = DisaggregatedDemandSumValidation(
            "test_rule", "test_task", "demand.egon_demandregio_zensus_electricity"
        )
        sql = rule.sql(None)

        assert "WITH disaggregated AS" in sql
        assert "sector = 'residential'" in sql
        assert "demand.egon_demandregio_hh" in sql
        assert "ABS(d.disagg_sum - o.orig_sum)" in sql

    def test_sql_generation_custom_sector(self):
        rule = DisaggregatedDemandSumValidation(
            "test_rule",
            "test_task",
            "demand.egon_demandregio_zensus_electricity",
            sector="commercial",
        )
        sql = rule.sql(None)

        assert "sector = 'commercial'" in sql

    def test_postprocess_within_tolerance(self):
        rule = DisaggregatedDemandSumValidation(
            "test_rule",
            "test_task",
            "demand.egon_demandregio_zensus_electricity",
            tolerance=0.05,
        )

        row = {
            "scenario": "eGon2035",
            "disagg_sum": 1000.0,
            "orig_sum": 1020.0,  # 2% difference
            "abs_diff": 20.0,
            "rel_diff": 0.0196,  # Within 5% tolerance
        }

        result = rule.postprocess(row, None)

        assert result.success is True
        assert "Scenario eGon2035" in result.message
        assert "Rel. diff 0.0196" in result.message
        assert result.observed == 0.0196
        assert result.expected == 0.0

    def test_postprocess_outside_tolerance(self):
        rule = DisaggregatedDemandSumValidation(
            "test_rule",
            "test_task",
            "demand.egon_demandregio_zensus_electricity",
            tolerance=0.01,
        )

        row = {
            "scenario": "eGon2035",
            "disagg_sum": 1000.0,
            "orig_sum": 1050.0,  # 5% difference
            "abs_diff": 50.0,
            "rel_diff": 0.0476,  # Outside 1% tolerance
        }

        result = rule.postprocess(row, None)

        assert result.success is False
        assert result.observed == 0.0476

    def test_postprocess_default_tolerance(self):
        # Use actual config default tolerance: DISAGGREGATED_DEMAND_TOLERANCE = 0.01
        rule = DisaggregatedDemandSumValidation(
            "test_rule", "test_task", "demand.egon_demandregio_zensus_electricity"
        )

        row = {
            "scenario": "test",
            "disagg_sum": 1000.0,
            "orig_sum": 1005.0,  # 0.5% difference
            "abs_diff": 5.0,
            "rel_diff": 0.005,  # Within 1% default tolerance
        }

        result = rule.postprocess(row, None)

        assert result.success is True
        assert "tolerance 0.01" in result.message

    def test_postprocess_none_values(self):
        rule = DisaggregatedDemandSumValidation(
            "test_rule", "test_task", "demand.egon_demandregio_zensus_electricity"
        )

        row = {
            "scenario": None,
            "disagg_sum": None,
            "orig_sum": None,
            "abs_diff": None,
            "rel_diff": None,
        }

        result = rule.postprocess(row, None)

        # Should handle None values gracefully
        assert "Scenario None" in result.message  # scenario info is in the message
        assert result.observed == 0.0

    def test_postprocess_zero_original_sum(self):
        rule = DisaggregatedDemandSumValidation(
            "test_rule", "test_task", "demand.egon_demandregio_zensus_electricity"
        )

        row = {
            "scenario": "test",
            "disagg_sum": 0.0,
            "orig_sum": 0.0,
            "abs_diff": 0.0,
            "rel_diff": 0.0,  # Would be NaN if not handled by NULLIF
        }

        result = rule.postprocess(row, None)

        assert result.success is True
        assert result.observed == 0.0
