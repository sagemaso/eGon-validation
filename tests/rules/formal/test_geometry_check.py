import pytest
from egon_validation.rules.formal.geometry_check import GeometryContainmentValidation
from egon_validation.rules.base import Severity


class TestGeometryContainmentValidation:
    def test_sql_generation_default_parameters(self):
        rule = GeometryContainmentValidation(
            "test_rule",
            "test_task",
            "supply.egon_power_plants_wind",
            reference_dataset="boundaries.vg250_sta"
        )
        sql = rule.sql(None)

        assert "WITH reference_geom AS" in sql
        assert "ST_Union(ST_Transform(geometry, 3035))" in sql  # default reference_geometry
        assert "boundaries.vg250_sta" in sql
        assert "WHERE TRUE" in sql  # default filters
        assert "ST_Contains" in sql
        assert "ST_Transform(points.geom, 3035)" in sql  # default geometry_column
        assert "supply.egon_power_plants_wind AS points" in sql
        assert "total_points" in sql
        assert "points_inside" in sql
        assert "points_outside" in sql

    def test_sql_generation_custom_parameters(self):
        rule = GeometryContainmentValidation(
            "test_rule",
            "test_task",
            "supply.egon_power_plants_wind",
            geometry_column="location",
            reference_dataset="boundaries.vg250_lan",
            reference_geometry="geom_polygon",
            reference_filter="state_id = 'NW'",
            filter_condition="capacity_mw > 5.0"
        )
        sql = rule.sql(None)

        assert "ST_Transform(geom_polygon, 3035)" in sql
        assert "boundaries.vg250_lan" in sql
        assert "WHERE state_id = 'NW'" in sql
        assert "ST_Transform(points.location, 3035)" in sql
        assert "points.capacity_mw > 5.0" in sql

    def test_postprocess_all_points_inside_boundary(self):
        """Test with realistic mock data: all wind plants within Germany"""
        rule = GeometryContainmentValidation(
            "wind_plants_germany",
            "geometry_validation",
            "supply.egon_power_plants_wind",
            geometry_column="geom",
            reference_dataset="boundaries.vg250_sta",
            reference_geometry="geometry",
            reference_filter="nuts = 'DE' AND gf = 4",
            filter_condition="site_type = 'Windkraft an Land'"
        )

        # Simulate DB result: all 15000 wind plants are within Germany
        mock_db_row = {
            "total_points": 15000,
            "points_inside": 15000,
            "points_outside": 0
        }

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - all points within boundary
        assert result.success is True
        assert result.message == "All 15000 points are within reference boundary (filter: site_type = 'Windkraft an Land')"
        assert result.rule_id == "wind_plants_germany"
        assert result.task == "geometry_validation"
        assert result.dataset == "supply.egon_power_plants_wind"
        assert result.column == "geom"
        assert result.observed == 0.0
        assert result.expected == 0.0
        assert result.severity == Severity.WARNING

    def test_postprocess_some_points_outside_boundary(self):
        """Test with realistic mock data: some wind plants outside Germany"""
        rule = GeometryContainmentValidation(
            "wind_plants_germany",
            "geometry_validation",
            "supply.egon_power_plants_wind",
            geometry_column="geom",
            reference_dataset="boundaries.vg250_sta",
            reference_geometry="geometry",
            reference_filter="nuts = 'DE' AND gf = 4",
            filter_condition="site_type = 'Windkraft an Land'"
        )

        # Simulate DB result: 25 wind plants are outside Germany (data quality issue)
        mock_db_row = {
            "total_points": 15025,
            "points_inside": 15000,
            "points_outside": 25
        }

        result = rule.postprocess(mock_db_row, None)

        # Should fail - points outside boundary detected
        assert result.success is False
        assert "25 points are outside reference boundary" in result.message
        assert "(15000 inside)" in result.message
        assert "[Filter: site_type = 'Windkraft an Land', Ref: nuts = 'DE' AND gf = 4]" in result.message
        # The debugging SQL is only included for the specific WIND_PLANTS_IN_GERMANY rule_id
        # This test uses "wind_plants_germany" so no debugging SQL should be included
        assert result.observed == 25.0
        assert result.rule_id == "wind_plants_germany"

    def test_postprocess_with_custom_filter(self):
        """Test with custom geometry filter conditions"""
        rule = GeometryContainmentValidation(
            "large_plants_check",
            "geometry_validation",
            "supply.egon_power_plants_wind",
            filter_condition="capacity_mw >= 10.0"
        )

        mock_db_row = {
            "total_points": 5000,
            "points_inside": 4980,
            "points_outside": 20
        }

        result = rule.postprocess(mock_db_row, None)

        assert result.success is False
        assert "20 points are outside reference boundary" in result.message
        assert "capacity_mw >= 10.0" in result.message
        assert result.observed == 20.0

    def test_postprocess_none_values_handling(self):
        """Test handling of None values in database result"""
        rule = GeometryContainmentValidation(
            "test_rule",
            "test_task",
            "test.table",
            reference_dataset="test.boundary"
        )

        mock_db_row = {
            "total_points": None,
            "points_inside": None,
            "points_outside": None
        }

        result = rule.postprocess(mock_db_row, None)

        # Should handle None values gracefully
        assert result.success is True  # 0 points_outside = success
        assert result.message == "All 0 points are within reference boundary (filter: TRUE)"
        assert result.observed == 0.0

    def test_postprocess_empty_result_set(self):
        """Test when no points match the filter condition"""
        rule = GeometryContainmentValidation(
            "test_rule",
            "test_task",
            "supply.egon_power_plants_wind",
            filter_condition="site_type = 'NonExistentType'"
        )

        mock_db_row = {
            "total_points": 0,
            "points_inside": 0,
            "points_outside": 0
        }

        result = rule.postprocess(mock_db_row, None)

        assert result.success is True
        assert "All 0 points are within reference boundary" in result.message
        assert result.observed == 0.0

    def test_with_mock_data_success_solar_plants(self):
        """Test with realistic mock data: solar plants within state boundary"""
        rule = GeometryContainmentValidation(
            "solar_plants_nrw",
            "regional_validation",
            "supply.egon_power_plants_pv",
            geometry_column="geom",
            reference_dataset="boundaries.vg250_lan",
            reference_geometry="geometry",
            reference_filter="nuts LIKE 'DE5%'",  # NRW
            filter_condition="site_type = 'Photovoltaik'"
        )

        # Simulate DB result: all solar plants in NRW are within NRW boundary
        mock_db_row = {
            "total_points": 8500,
            "points_inside": 8500,
            "points_outside": 0
        }

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - perfect regional data
        assert result.success is True
        assert result.message == "All 8500 points are within reference boundary (filter: site_type = 'Photovoltaik')"
        assert result.rule_id == "solar_plants_nrw"
        assert result.task == "regional_validation"
        assert result.dataset == "supply.egon_power_plants_pv"

    def test_with_mock_data_failure_cross_border_plants(self):
        """Test with realistic mock data: plants incorrectly assigned to wrong region"""
        rule = GeometryContainmentValidation(
            "plants_bavaria",
            "regional_validation",
            "supply.egon_power_plants_biomass",
            geometry_column="geom",
            reference_dataset="boundaries.vg250_lan",
            reference_geometry="geometry",
            reference_filter="nuts LIKE 'DE2%'",  # Bavaria
            filter_condition="federal_state = 'Bayern'"
        )

        # Simulate DB result: some plants marked as "Bayern" are outside Bavaria boundary
        mock_db_row = {
            "total_points": 1200,
            "points_inside": 1180,
            "points_outside": 20
        }

        result = rule.postprocess(mock_db_row, None)

        # Should fail - geographic inconsistency detected
        assert result.success is False
        assert "20 points are outside reference boundary" in result.message
        assert "(1180 inside)" in result.message
        assert "federal_state = 'Bayern'" in result.message
        assert result.observed == 20.0
        assert result.rule_id == "plants_bavaria"

    def test_postprocess_wind_plants_debugging_info(self):
        """Test that debugging SQL is included for wind plants rule specifically"""
        rule = GeometryContainmentValidation("WIND_PLANTS_IN_GERMANY", "test_task", "test.table")

        mock_db_row = {
            "total_points": 100,
            "points_inside": 90,
            "points_outside": 10
        }

        result = rule.postprocess(mock_db_row, None)

        # Should include debugging SQL for wind plants rule
        assert "SELECT * FROM supply.egon_power_plants_wind" in result.message
        assert "site_type = 'Windkraft an Land'" in result.message
        assert "ST_Covers" in result.message

    def test_postprocess_non_wind_plants_no_debugging_info(self):
        """Test that debugging SQL is NOT included for other rules"""
        rule = GeometryContainmentValidation("OTHER_RULE", "test_task", "test.table")

        mock_db_row = {
            "total_points": 100,
            "points_inside": 90,
            "points_outside": 10
        }

        result = rule.postprocess(mock_db_row, None)

        # Should NOT include debugging SQL for non-wind plants rules
        assert "SELECT * FROM supply.egon_power_plants_wind" not in result.message