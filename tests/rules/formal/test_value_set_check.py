import pytest
from egon_validation.rules.formal.value_set_check import ValueSetValidation
from egon_validation.rules.base import Severity


class TestValueSetValidation:
    def test_sql_generation(self):
        rule = ValueSetValidation(
            "test_rule",
            "test_task",
            "test.table",
            column="status",
            expected_values=["active", "inactive"],
        )
        sql = rule.sql(None)

        assert "ARRAY['active','inactive']" in sql
        assert "COUNT(*) as total_rows" in sql
        assert "COUNT(CASE WHEN status = ANY" in sql

    def test_sql_generation_empty_values(self):
        rule = ValueSetValidation("test_rule", "test_task", "test.table")
        sql = rule.sql(None)

        assert "ARRAY[]" in sql

    def test_postprocess_all_valid(self):
        rule = ValueSetValidation(
            "test_rule",
            "test_task",
            "test.table",
            expected_values=["active", "inactive"],
        )
        row = {
            "total_rows": 100,
            "valid_values": 100,
            "invalid_values": 0,
            "invalid_distinct": [],
        }

        result = rule.postprocess(row, None)

        assert result.success is True
        assert "All 100 values are in expected set" in result.message
        assert result.observed == 0
        assert result.expected == 0.0

    def test_postprocess_invalid_values(self):
        rule = ValueSetValidation(
            "test_rule",
            "test_task",
            "test.table",
            expected_values=["active", "inactive"],
        )
        row = {
            "total_rows": 100,
            "valid_values": 95,
            "invalid_values": 5,
            "invalid_distinct": ["pending", "unknown"],
        }

        result = rule.postprocess(row, None)

        assert result.success is False
        assert "5 invalid values found" in result.message
        assert "Invalid values: ['pending', 'unknown']" in result.message
        assert result.observed == 5

    def test_postprocess_none_values(self):
        rule = ValueSetValidation("test_rule", "test_task", "test.table")
        row = {
            "total_rows": None,
            "valid_values": None,
            "invalid_values": None,
            "invalid_distinct": None,
        }

        result = rule.postprocess(row, None)

        assert result.success is True
        assert result.observed == 0

    def test_with_mock_data_success_all_values_valid(self):
        """Test with realistic mock data: all values are in expected set"""
        # Mock validating scenario column with valid scenario names
        rule = ValueSetValidation(
            "scenario_value_check",
            "data_validation",
            "demand.egon_demandregio_hh",
            column="scenario",
            expected_values=["eGon2035", "eGon100RE", "eGon2021"]
        )

        # Simulate DB result: all 50000 rows have valid scenario values
        mock_db_row = {
            "total_rows": 50000,
            "valid_values": 50000,
            "invalid_values": 0,
            "invalid_distinct": []
        }

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - all values are valid
        assert result.success is True
        assert "All 50000 values are in expected set" in result.message
        assert result.observed == 0
        assert result.expected == 0.0
        assert result.rule_id == "scenario_value_check"
        assert result.task == "data_validation"
        assert result.dataset == "demand.egon_demandregio_hh"
        assert result.column == "scenario"

    def test_with_mock_data_failure_invalid_values_found(self):
        """Test with realistic mock data: some values are not in expected set"""
        # Mock validating carrier column with unexpected carrier types
        rule = ValueSetValidation(
            "carrier_value_check",
            "data_validation",
            "grid.egon_etrago_load",
            column="carrier",
            expected_values=["AC", "heat", "CH4", "H2"]
        )

        # Simulate DB result: 89 rows have invalid carrier values
        mock_db_row = {
            "total_rows": 12567,
            "valid_values": 12478,
            "invalid_values": 89,
            "invalid_distinct": ["biomass", "oil", "unknown"]
        }

        result = rule.postprocess(mock_db_row, None)

        # Should fail - invalid values detected
        assert result.success is False
        assert "89 invalid values found" in result.message
        assert "Invalid values: ['biomass', 'oil', 'unknown']" in result.message
        assert result.observed == 89
        assert result.rule_id == "carrier_value_check"
        assert result.dataset == "grid.egon_etrago_load"
        assert result.column == "carrier"