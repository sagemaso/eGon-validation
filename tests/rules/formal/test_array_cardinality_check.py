import pytest
from egon_validation.rules.formal.array_cardinality_check import ArrayCardinalityValidation
from egon_validation.rules.base import Severity


class TestArrayCardinalityValidation:
    def test_sql_generation_default_parameters(self):
        rule = ArrayCardinalityValidation(
            "test_rule", "test_task", "grid.egon_etrago_load_timeseries"
        )
        sql = rule.sql(None)

        assert "COUNT(*) as total_rows" in sql
        assert "cardinality(values)" in sql  # default array_column
        assert "8760" in sql  # default expected_length from config
        assert "grid.egon_etrago_load_timeseries" in sql
        assert "correct_length" in sql
        assert "wrong_length" in sql
        assert "null_arrays" in sql

    def test_sql_generation_custom_parameters(self):
        rule = ArrayCardinalityValidation(
            "test_rule",
            "test_task",
            "demand.egon_heat_timeseries_selected_profiles",
            array_column="selected_idp_profiles",
            expected_length=365
        )
        sql = rule.sql(None)

        assert "cardinality(selected_idp_profiles)" in sql
        assert "365" in sql  # custom expected length
        assert "demand.egon_heat_timeseries_selected_profiles" in sql

    def test_postprocess_all_arrays_correct_length(self):
        """Test with realistic mock data: all arrays have correct length"""
        rule = ArrayCardinalityValidation(
            "load_timeseries_validation",
            "data_quality",
            "grid.egon_etrago_load_timeseries",
            array_column="p_set",
            expected_length=8760
        )

        # Simulate DB result: all 1000 arrays have correct length of 8760
        mock_db_row = {
            "total_rows": 1000,
            "correct_length": 1000,
            "wrong_length": 0,
            "null_arrays": 0,
            "found_lengths": [8760],
            "min_length": 8760,
            "max_length": 8760,
            "avg_length": 8760.0
        }

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - all arrays have correct length
        assert result.success is True
        assert result.message == "All 1000 arrays have correct length of 8760"
        assert result.rule_id == "load_timeseries_validation"
        assert result.task == "data_quality"
        assert result.dataset == "grid.egon_etrago_load_timeseries"
        assert result.column == "p_set"
        assert result.observed == 0.0
        assert result.expected == 0.0
        assert result.severity == Severity.WARNING

    def test_postprocess_some_arrays_wrong_length(self):
        """Test with realistic mock data: some arrays have wrong length"""
        rule = ArrayCardinalityValidation(
            "heat_profiles_validation",
            "data_quality",
            "demand.egon_heat_timeseries_selected_profiles",
            array_column="selected_idp_profiles",
            expected_length=365
        )

        # Simulate DB result: 50 arrays have wrong length, 5 are NULL
        mock_db_row = {
            "total_rows": 1000,
            "correct_length": 945,
            "wrong_length": 50,
            "null_arrays": 5,
            "found_lengths": [365, 364, 366, None],
            "min_length": 364,
            "max_length": 366,
            "avg_length": 365.1
        }

        result = rule.postprocess(mock_db_row, None)

        # Should fail - arrays with wrong length detected
        assert result.success is False
        assert "50 arrays with wrong length" in result.message
        assert "5 NULL arrays" in result.message
        assert "Expected: 365" in result.message
        assert "[365, 364, 366, None]" in result.message
        assert "Range: 364-366" in result.message
        assert "Avg: 365.10" in result.message
        assert result.observed == 50.0
        assert result.rule_id == "heat_profiles_validation"
        assert result.dataset == "demand.egon_heat_timeseries_selected_profiles"

    def test_postprocess_only_wrong_length_no_nulls(self):
        """Test with only wrong length arrays, no NULL arrays"""
        rule = ArrayCardinalityValidation(
            "test_rule", "test_task", "test.table", expected_length=8760
        )

        mock_db_row = {
            "total_rows": 100,
            "correct_length": 80,
            "wrong_length": 20,
            "null_arrays": 0,
            "found_lengths": [8760, 8759, 8761],
            "min_length": 8759,
            "max_length": 8761,
            "avg_length": 8759.8
        }

        result = rule.postprocess(mock_db_row, None)

        assert result.success is False
        assert "20 arrays with wrong length" in result.message
        assert "NULL arrays" not in result.message
        assert result.observed == 20.0

    def test_postprocess_only_null_arrays_no_wrong_length(self):
        """Test with only NULL arrays, no wrong length arrays"""
        rule = ArrayCardinalityValidation(
            "test_rule", "test_task", "test.table", expected_length=8760
        )

        mock_db_row = {
            "total_rows": 100,
            "correct_length": 85,
            "wrong_length": 0,
            "null_arrays": 15,
            "found_lengths": [8760, None],
            "min_length": 8760,
            "max_length": 8760,
            "avg_length": 8760.0
        }

        result = rule.postprocess(mock_db_row, None)

        assert result.success is False
        assert "15 NULL arrays" in result.message
        assert "wrong length" not in result.message
        assert result.observed == 0.0  # wrong_length is 0

    def test_postprocess_none_values_handling(self):
        """Test handling of None values in database result"""
        rule = ArrayCardinalityValidation(
            "test_rule", "test_task", "test.table", expected_length=8760
        )

        mock_db_row = {
            "total_rows": None,
            "correct_length": None,
            "wrong_length": None,
            "null_arrays": None,
            "found_lengths": None,
            "min_length": None,
            "max_length": None,
            "avg_length": None
        }

        result = rule.postprocess(mock_db_row, None)

        # Should handle None values gracefully
        assert result.success is True  # 0 wrong_length and 0 null_arrays = success
        assert result.message == "All 0 arrays have correct length of 8760"
        assert result.observed == 0.0

    def test_postprocess_empty_found_lengths(self):
        """Test when found_lengths is empty or None"""
        rule = ArrayCardinalityValidation(
            "test_rule", "test_task", "test.table", expected_length=8760
        )

        mock_db_row = {
            "total_rows": 10,
            "correct_length": 8,
            "wrong_length": 2,
            "null_arrays": 0,
            "found_lengths": [],  # empty list
            "min_length": 8759,
            "max_length": 8760,
            "avg_length": 8759.5
        }

        result = rule.postprocess(mock_db_row, None)

        assert result.success is False
        assert "Expected: 8760, Found lengths: []" in result.message
        assert result.observed == 2.0

    def test_with_mock_data_success_annual_timeseries(self):
        """Test with realistic mock data: annual timeseries with 8760 hours"""
        rule = ArrayCardinalityValidation(
            "generator_timeseries_check",
            "timeseries_validation",
            "grid.egon_etrago_generator_timeseries",
            array_column="p_max_pu",
            expected_length=8760
        )

        # Simulate DB result: perfect annual timeseries data
        mock_db_row = {
            "total_rows": 5432,
            "correct_length": 5432,
            "wrong_length": 0,
            "null_arrays": 0,
            "found_lengths": [8760],
            "min_length": 8760,
            "max_length": 8760,
            "avg_length": 8760.0
        }

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - perfect annual data
        assert result.success is True
        assert result.message == "All 5432 arrays have correct length of 8760"
        assert result.rule_id == "generator_timeseries_check"
        assert result.task == "timeseries_validation"
        assert result.dataset == "grid.egon_etrago_generator_timeseries"
        assert result.column == "p_max_pu"

    def test_with_mock_data_failure_corrupted_timeseries(self):
        """Test with realistic mock data: corrupted timeseries with inconsistent lengths"""
        rule = ArrayCardinalityValidation(
            "bus_timeseries_check",
            "timeseries_validation",
            "grid.egon_etrago_bus_timeseries",
            array_column="v_mag_pu_set",
            expected_length=8760
        )

        # Simulate DB result: corrupted data with various issues
        mock_db_row = {
            "total_rows": 2000,
            "correct_length": 1850,
            "wrong_length": 100,
            "null_arrays": 50,
            "found_lengths": [8760, 8759, 8761, 4380, None],  # Some half-year data
            "min_length": 4380,
            "max_length": 8761,
            "avg_length": 8456.2
        }

        result = rule.postprocess(mock_db_row, None)

        # Should fail - data quality issues detected
        assert result.success is False
        assert "100 arrays with wrong length" in result.message
        assert "50 NULL arrays" in result.message
        assert "Expected: 8760" in result.message
        assert "Range: 4380-8761" in result.message
        assert "Avg: 8456.20" in result.message
        assert result.observed == 100.0
        assert result.rule_id == "bus_timeseries_check"
        assert result.dataset == "grid.egon_etrago_bus_timeseries"
        assert result.column == "v_mag_pu_set"