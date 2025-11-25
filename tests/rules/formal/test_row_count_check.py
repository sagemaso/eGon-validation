import pytest
from egon_validation.rules.formal.row_count_check import RowCountValidation, RowCountComparisonValidation
from egon_validation.rules.base import Severity


class TestRowCountValidation:
    def test_sql_generation(self):
        rule = RowCountValidation(
            "test_rule", "test_task", "grid.egon_mv_grid_district"
        )
        sql = rule.sql(None)

        assert sql == "SELECT COUNT(*) AS actual_count FROM grid.egon_mv_grid_district"

    def test_postprocess_correct_count(self):
        """Test with realistic mock data: table has expected row count"""
        rule = RowCountValidation(
            "mv_grid_count_check",
            "data_integrity",
            "grid.egon_mv_grid_district",
            expected_count=3854
        )

        # Simulate DB result: exactly the expected number of MV grid districts
        mock_db_row = {"actual_count": 3854}

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - correct count
        assert result.success is True
        assert result.message == "Expected 3854 rows, found 3854"
        assert result.rule_id == "mv_grid_count_check"
        assert result.task == "data_integrity"
        assert result.dataset == "grid.egon_mv_grid_district"
        assert result.observed == 3854.0
        assert result.expected == 3854.0
        assert result.severity == Severity.INFO  # Success results in INFO severity

    def test_postprocess_incorrect_count(self):
        """Test with realistic mock data: table has wrong row count"""
        rule = RowCountValidation(
            "mv_grid_count_check",
            "data_integrity",
            "grid.egon_mv_grid_district",
            expected_count=3854
        )

        # Simulate DB result: fewer MV grid districts than expected (data loss)
        mock_db_row = {"actual_count": 3820}

        result = rule.postprocess(mock_db_row, None)

        # Should fail - incorrect count
        assert result.success is False
        assert result.message == "Expected 3854 rows, found 3820"
        assert result.observed == 3820.0
        assert result.expected == 3854.0
        assert result.rule_id == "mv_grid_count_check"

    def test_postprocess_none_value_handling(self):
        """Test handling of None value in database result"""
        rule = RowCountValidation(
            "test_rule", "test_task", "test.table", expected_count=100
        )

        mock_db_row = {"actual_count": None}

        result = rule.postprocess(mock_db_row, None)

        # Should handle None gracefully - treat as 0
        assert result.success is False
        assert result.message == "Expected 100 rows, found 0"
        assert result.observed == 0.0

    def test_with_mock_data_success_boundary_table(self):
        """Test with realistic mock data: boundary table with correct count"""
        rule = RowCountValidation(
            "german_states_count",
            "boundary_validation",
            "boundaries.vg250_lan",
            expected_count=16
        )

        # Simulate DB result: exactly 16 German states
        mock_db_row = {"actual_count": 16}

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - correct state count
        assert result.success is True
        assert result.message == "Expected 16 rows, found 16"
        assert result.rule_id == "german_states_count"
        assert result.task == "boundary_validation"

    def test_with_mock_data_failure_missing_data(self):
        """Test with realistic mock data: data import incomplete"""
        rule = RowCountValidation(
            "power_plants_count",
            "import_validation",
            "supply.egon_power_plants_wind",
            expected_count=25000
        )

        # Simulate DB result: import was interrupted, missing data
        mock_db_row = {"actual_count": 18500}

        result = rule.postprocess(mock_db_row, None)

        # Should fail - data import incomplete
        assert result.success is False
        assert result.message == "Expected 25000 rows, found 18500"
        assert result.observed == 18500.0
        assert result.expected == 25000.0


class TestRowCountComparisonValidation:
    def test_sql_generation(self):
        rule = RowCountComparisonValidation(
            "test_rule",
            "test_task",
            "demand.egon_demandregio_cts_ind",
            scenario_col="scenario",
            economic_sector_col="wz",
            reference_dataset="boundaries.vg250_krs",
            reference_filter="gf = 4"
        )
        sql = rule.sql(None)

        assert "WITH reference_count AS" in sql
        assert "boundaries.vg250_krs WHERE gf = 4" in sql
        assert "GROUP BY scenario, wz" in sql
        assert "matching_groups" in sql
        assert "mismatching_groups" in sql
        assert "array_agg(DISTINCT g.group_count)" in sql

    def test_postprocess_all_groups_match(self):
        """Test with realistic mock data: all scenario-sector groups have correct count"""
        rule = RowCountComparisonValidation(
            "cts_ind_coverage_check",
            "data_completeness",
            "demand.egon_demandregio_cts_ind",
            scenario_col="scenario",
            economic_sector_col="wz",
            reference_dataset="boundaries.vg250_krs",
            reference_filter="gf = 4"
        )

        # Simulate DB result: all groups have exactly 401 entries (one per Kreis)
        mock_db_row = {
            "ref_count": 401,
            "total_groups": 20,  # 2 scenarios * 10 economic sectors
            "matching_groups": 20,
            "mismatching_groups": 0,
            "found_counts": [401]
        }

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - complete coverage
        assert result.success is True
        assert result.message == "All 20 groups have expected count 401"
        assert result.rule_id == "cts_ind_coverage_check"
        assert result.task == "data_completeness"
        assert result.dataset == "demand.egon_demandregio_cts_ind"
        assert result.observed == 0.0
        assert result.expected == 0.0
        assert result.severity == Severity.INFO  # Success results in INFO severity

    def test_postprocess_some_groups_mismatch(self):
        """Test with realistic mock data: some groups have incomplete data"""
        rule = RowCountComparisonValidation(
            "cts_ind_coverage_check",
            "data_completeness",
            "demand.egon_demandregio_cts_ind",
            scenario_col="scenario",
            economic_sector_col="wz"
        )

        # Simulate DB result: some groups missing data for certain Kreise
        mock_db_row = {
            "ref_count": 401,
            "total_groups": 20,
            "matching_groups": 17,
            "mismatching_groups": 3,
            "found_counts": [401, 398, 399, 400]  # Some groups have missing entries
        }

        result = rule.postprocess(mock_db_row, None)

        # Should fail - incomplete coverage detected
        assert result.success is False
        assert "3/20 groups have wrong count" in result.message
        assert "Expected: 401" in result.message
        assert "[401, 398, 399, 400]" in result.message
        assert result.observed == 3.0
        assert result.rule_id == "cts_ind_coverage_check"

    def test_postprocess_none_values_handling(self):
        """Test handling of None values in database result"""
        rule = RowCountComparisonValidation(
            "test_rule", "test_task", "test.table"
        )

        mock_db_row = {
            "ref_count": None,
            "total_groups": None,
            "matching_groups": None,
            "mismatching_groups": None,
            "found_counts": None
        }

        result = rule.postprocess(mock_db_row, None)

        # Should handle None values gracefully
        assert result.success is True  # 0 mismatching_groups = success
        assert result.message == "All 0 groups have expected count 0"
        assert result.observed == 0.0

    def test_with_mock_data_success_demand_sectors(self):
        """Test with realistic mock data: demand data complete for all sectors"""
        rule = RowCountComparisonValidation(
            "residential_demand_coverage",
            "sector_validation",
            "demand.egon_demandregio_hh",
            scenario_col="scenario",
            economic_sector_col="sector",
            reference_dataset="boundaries.vg250_gem",
            reference_filter="gf = 4"
        )

        # Simulate DB result: residential demand covers all 11014 municipalities
        mock_db_row = {
            "ref_count": 11014,
            "total_groups": 6,  # 3 scenarios * 2 sectors
            "matching_groups": 6,
            "mismatching_groups": 0,
            "found_counts": [11014]
        }

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - complete municipal coverage
        assert result.success is True
        assert result.message == "All 6 groups have expected count 11014"
        assert result.rule_id == "residential_demand_coverage"
        assert result.task == "sector_validation"

    def test_with_mock_data_failure_missing_municipalities(self):
        """Test with realistic mock data: some municipalities missing demand data"""
        rule = RowCountComparisonValidation(
            "commercial_demand_coverage",
            "sector_validation",
            "demand.egon_demandregio_cts",
            scenario_col="scenario",
            economic_sector_col="sector"
        )

        # Simulate DB result: some rural municipalities missing commercial demand
        mock_db_row = {
            "ref_count": 11014,
            "total_groups": 4,  # 2 scenarios * 2 sectors
            "matching_groups": 2,
            "mismatching_groups": 2,
            "found_counts": [11014, 10950, 10978]  # Some municipalities missing
        }

        result = rule.postprocess(mock_db_row, None)

        # Should fail - incomplete municipal coverage
        assert result.success is False
        assert "2/4 groups have wrong count" in result.message
        assert "Expected: 11014" in result.message
        assert result.observed == 2.0