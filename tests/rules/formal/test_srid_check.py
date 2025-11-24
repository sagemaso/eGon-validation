import pytest
from egon_validation.rules.formal.srid_check import SRIDUniqueNonZero, SRIDSpecificValidation
from egon_validation.rules.base import Severity


class TestSRIDUniqueNonZero:
    def test_sql_generation_default_geom_column(self):
        rule = SRIDUniqueNonZero(
            "test_rule", "test_task", "supply.egon_power_plants_pv"
        )
        sql = rule.sql(None)

        assert "COUNT(DISTINCT ST_SRID(geom))" in sql
        assert "SUM(CASE WHEN ST_SRID(geom) = 0" in sql
        assert "supply.egon_power_plants_pv" in sql
        assert "srids" in sql
        assert "srid_zero" in sql

    def test_sql_generation_custom_geom_column(self):
        rule = SRIDUniqueNonZero(
            "test_rule", "test_task", "boundaries.vg250_sta", geom="geometry"
        )
        sql = rule.sql(None)

        assert "ST_SRID(geometry)" in sql
        assert "boundaries.vg250_sta" in sql

    def test_postprocess_single_srid_no_zeros(self):
        """Test with realistic mock data: all PV plants have consistent SRID"""
        rule = SRIDUniqueNonZero(
            "pv_srid_consistency",
            "geometry_validation",
            "supply.egon_power_plants_pv",
            geom="geom"
        )

        # Simulate DB result: all geometries have SRID 4326
        mock_db_row = {
            "srids": 1,  # Single SRID across all geometries
            "srid_zero": 0  # No geometries with SRID=0
        }

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - consistent SRID usage
        assert result.success is True
        assert result.message == "Exactly one SRID and none equals 0"
        assert result.rule_id == "pv_srid_consistency"
        assert result.task == "geometry_validation"
        assert result.dataset == "supply.egon_power_plants_pv"
        assert result.column == "geom"
        assert result.observed == 1.0
        assert result.expected == 1.0
        assert result.severity == Severity.INFO  # Success results in INFO severity

    def test_postprocess_multiple_srids(self):
        """Test with realistic mock data: mixed SRIDs in wind plant data"""
        rule = SRIDUniqueNonZero(
            "wind_srid_consistency",
            "geometry_validation",
            "supply.egon_power_plants_wind",
            geom="geom"
        )

        # Simulate DB result: inconsistent SRIDs (data quality issue)
        mock_db_row = {
            "srids": 3,  # Multiple SRIDs found
            "srid_zero": 0
        }

        result = rule.postprocess(mock_db_row, None)

        # Should fail - SRID inconsistency detected
        assert result.success is False
        assert result.observed == 3.0
        assert result.rule_id == "wind_srid_consistency"
        assert result.dataset == "supply.egon_power_plants_wind"

    def test_postprocess_zero_srids_present(self):
        """Test with realistic mock data: some geometries have SRID=0"""
        rule = SRIDUniqueNonZero(
            "boundary_srid_check",
            "geometry_validation",
            "boundaries.vg250_sta",
            geom="geometry"
        )

        # Simulate DB result: some geometries have undefined SRID
        mock_db_row = {
            "srids": 2,  # Mixed SRIDs including 0
            "srid_zero": 5  # 5 geometries with SRID=0
        }

        result = rule.postprocess(mock_db_row, None)

        # Should fail - undefined SRID detected
        assert result.success is False
        assert result.observed == 2.0
        assert result.rule_id == "boundary_srid_check"
        assert result.column == "geometry"

    def test_postprocess_none_values_handling(self):
        """Test handling of None values in database result"""
        rule = SRIDUniqueNonZero(
            "test_rule", "test_task", "test.table"
        )

        mock_db_row = {
            "srids": None,
            "srid_zero": None
        }

        result = rule.postprocess(mock_db_row, None)

        # Should handle None values gracefully
        assert result.success is False  # 0 SRIDs != 1 expected
        assert result.observed == 0.0
        assert result.expected == 1.0


class TestSRIDSpecificValidation:
    def test_sql_generation_default_parameters(self):
        rule = SRIDSpecificValidation(
            "test_rule", "test_task", "grid.egon_mv_grid_district"
        )
        sql = rule.sql(None)

        assert "COUNT(*) AS total_geometries" in sql
        assert "COUNT(DISTINCT ST_SRID(geom))" in sql
        assert "unique_srids" in sql
        assert "correct_srid_count" in sql
        assert "zero_srid_count" in sql
        assert "found_srids" in sql
        assert "grid.egon_mv_grid_district" in sql

    def test_sql_generation_custom_parameters(self):
        rule = SRIDSpecificValidation(
            "test_rule",
            "test_task",
            "boundaries.vg250_sta",
            geom="geometry",
            expected_srid=4326
        )
        sql = rule.sql(None)

        assert "ST_SRID(geometry)" in sql
        assert "4326" in sql
        assert "boundaries.vg250_sta" in sql

    def test_postprocess_all_correct_srid(self):
        """Test with realistic mock data: all MV grid districts have correct SRID 3035"""
        rule = SRIDSpecificValidation(
            "mv_grid_srid_validation",
            "geometry_validation",
            "grid.egon_mv_grid_district",
            geom="geom",
            expected_srid=3035
        )

        # Simulate DB result: all 3854 grid districts have SRID 3035
        mock_db_row = {
            "total_geometries": 3854,
            "unique_srids": 1,
            "correct_srid_count": 3854,
            "zero_srid_count": 0,
            "found_srids": [3035]
        }

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - correct SRID for all geometries
        assert result.success is True
        assert result.message == "All 3854 geometries have correct SRID 3035"
        assert result.rule_id == "mv_grid_srid_validation"
        assert result.task == "geometry_validation"
        assert result.dataset == "grid.egon_mv_grid_district"
        assert result.column == "geom"
        assert result.observed == 1.0
        assert result.expected == 1.0
        assert result.severity == Severity.INFO  # Success results in INFO severity

    def test_postprocess_wrong_srid(self):
        """Test with realistic mock data: wind plants have wrong SRID"""
        rule = SRIDSpecificValidation(
            "wind_srid_validation",
            "geometry_validation",
            "supply.egon_power_plants_wind",
            geom="geom",
            expected_srid=4326
        )

        # Simulate DB result: some plants have wrong SRID (projection issue)
        mock_db_row = {
            "total_geometries": 15000,
            "unique_srids": 2,
            "correct_srid_count": 14800,
            "zero_srid_count": 0,
            "found_srids": [4326, 3035]
        }

        result = rule.postprocess(mock_db_row, None)

        # Should fail - mixed SRIDs detected
        assert result.success is False
        assert "Multiple SRIDs found: [4326, 3035]" in result.message
        assert "Only 14800/15000 have expected SRID 4326" in result.message
        assert result.observed == 2.0
        assert result.rule_id == "wind_srid_validation"
        assert result.dataset == "supply.egon_power_plants_wind"

    def test_postprocess_zero_srid_issue(self):
        """Test with realistic mock data: boundary data has undefined SRID"""
        rule = SRIDSpecificValidation(
            "boundary_srid_validation",
            "geometry_validation",
            "boundaries.vg250_sta",
            geom="geometry",
            expected_srid=4326
        )

        # Simulate DB result: some boundaries have SRID=0 (import issue)
        mock_db_row = {
            "total_geometries": 16,
            "unique_srids": 2,
            "correct_srid_count": 15,
            "zero_srid_count": 1,
            "found_srids": [4326, 0]
        }

        result = rule.postprocess(mock_db_row, None)

        # Should fail - undefined SRID detected
        assert result.success is False
        assert "Multiple SRIDs found: [4326, 0]" in result.message
        assert "Only 15/16 have expected SRID 4326" in result.message
        assert "1 geometries with SRID=0" in result.message
        assert result.observed == 2.0
        assert result.rule_id == "boundary_srid_validation"

    def test_postprocess_multiple_problems(self):
        """Test with multiple SRID problems combined"""
        rule = SRIDSpecificValidation(
            "test_rule",
            "test_task",
            "test.table",
            expected_srid=3035
        )

        # Simulate DB result: multiple issues
        mock_db_row = {
            "total_geometries": 1000,
            "unique_srids": 3,
            "correct_srid_count": 800,
            "zero_srid_count": 50,
            "found_srids": [3035, 4326, 0]
        }

        result = rule.postprocess(mock_db_row, None)

        assert result.success is False
        assert "Multiple SRIDs found: [3035, 4326, 0]" in result.message
        assert "Only 800/1000 have expected SRID 3035" in result.message
        assert "50 geometries with SRID=0" in result.message
        assert result.observed == 3.0

    def test_postprocess_none_values_handling(self):
        """Test handling of None values in database result"""
        rule = SRIDSpecificValidation(
            "test_rule", "test_task", "test.table", expected_srid=4326
        )

        mock_db_row = {
            "total_geometries": None,
            "unique_srids": None,
            "correct_srid_count": None,
            "zero_srid_count": None,
            "found_srids": None
        }

        result = rule.postprocess(mock_db_row, None)

        # Should handle None values gracefully - but 0 unique_srids != 1 expected
        assert result.success is False
        assert "Multiple SRIDs found: None" in result.message
        assert result.observed == 0.0

    def test_with_mock_data_success_perfect_boundaries(self):
        """Test with realistic mock data: German states with perfect SRID"""
        rule = SRIDSpecificValidation(
            "german_states_srid",
            "boundary_validation",
            "boundaries.vg250_lan",
            geom="geometry",
            expected_srid=4326
        )

        # Simulate DB result: perfect boundary data
        mock_db_row = {
            "total_geometries": 16,  # 16 German states
            "unique_srids": 1,
            "correct_srid_count": 16,
            "zero_srid_count": 0,
            "found_srids": [4326]
        }

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - perfect boundary data
        assert result.success is True
        assert result.message == "All 16 geometries have correct SRID 4326"
        assert result.rule_id == "german_states_srid"
        assert result.task == "boundary_validation"

    def test_with_mock_data_failure_mixed_projections(self):
        """Test with realistic mock data: power plants with mixed projections"""
        rule = SRIDSpecificValidation(
            "pv_plants_srid",
            "geometry_validation",
            "supply.egon_power_plants_pv",
            geom="geom",
            expected_srid=4326
        )

        # Simulate DB result: mixed coordinate systems (data migration issue)
        mock_db_row = {
            "total_geometries": 25000,
            "unique_srids": 2,
            "correct_srid_count": 24500,
            "zero_srid_count": 0,
            "found_srids": [4326, 31467]  # Mix of WGS84 and Gauss-Krüger
        }

        result = rule.postprocess(mock_db_row, None)

        # Should fail - coordinate system inconsistency
        assert result.success is False
        assert "Multiple SRIDs found: [4326, 31467]" in result.message
        assert "Only 24500/25000 have expected SRID 4326" in result.message
        assert result.observed == 2.0
        assert result.rule_id == "pv_plants_srid"