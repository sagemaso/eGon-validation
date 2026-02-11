from unittest.mock import patch

from egon_validation.rules.custom.numeric_aggregation_check import (
    ElectricalLoadAggregationValidation,
)
from egon_validation.rules.base import Severity


class TestElectricalLoadAggregationValidation:
    def test_sql_generation(self):
        rule = ElectricalLoadAggregationValidation(
            "test_rule", "test_task", "grid.egon_etrago_load", tolerance=0.05
        )
        sql = rule.get_query(None)

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
