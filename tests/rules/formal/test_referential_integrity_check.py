import pytest
from egon_validation.rules.formal.referential_integrity_check import ReferentialIntegrityValidation
from egon_validation.rules.base import Severity


class TestReferentialIntegrityValidation:
    def test_sql_generation_default_parameters(self):
        rule = ReferentialIntegrityValidation(
            "test_rule",
            "test_task",
            "grid.egon_etrago_load_timeseries",
            reference_dataset="grid.egon_etrago_load"
        )
        sql = rule.sql(None)

        assert "COUNT(*) FILTER" in sql
        assert "total_non_null_references" in sql
        assert "valid_references" in sql
        assert "orphaned_references" in sql
        assert "grid.egon_etrago_load_timeseries as child" in sql
        assert "grid.egon_etrago_load as parent" in sql
        assert "child.id = parent.id" in sql  # default columns

    def test_sql_generation_custom_parameters(self):
        rule = ReferentialIntegrityValidation(
            "test_rule",
            "test_task",
            "grid.egon_etrago_load_timeseries",
            foreign_column="load_id",
            reference_dataset="grid.egon_etrago_load",
            reference_column="load_id"
        )
        sql = rule.sql(None)

        assert "child.load_id" in sql
        assert "parent.load_id" in sql
        assert "child.load_id = parent.load_id" in sql

    def test_postprocess_all_references_valid(self):
        """Test with realistic mock data: all load timeseries have valid load references"""
        rule = ReferentialIntegrityValidation(
            "load_timeseries_integrity",
            "data_integrity",
            "grid.egon_etrago_load_timeseries",
            foreign_column="load_id",
            reference_dataset="grid.egon_etrago_load",
            reference_column="load_id"
        )

        # Simulate DB result: all 5000 load timeseries reference valid loads
        mock_db_row = {
            "total_non_null_references": 5000,
            "valid_references": 5000,
            "orphaned_references": 0
        }

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - perfect referential integrity
        assert result.success is True
        assert result.message == "All 5000 references in load_id have valid matches in grid.egon_etrago_load.load_id"
        assert result.rule_id == "load_timeseries_integrity"
        assert result.task == "data_integrity"
        assert result.dataset == "grid.egon_etrago_load_timeseries"
        assert result.column == "load_id"
        assert result.observed == 0.0
        assert result.expected == 0.0
        assert result.severity == Severity.WARNING

    def test_postprocess_orphaned_references(self):
        """Test with realistic mock data: some timeseries reference non-existent loads"""
        rule = ReferentialIntegrityValidation(
            "load_timeseries_integrity",
            "data_integrity",
            "grid.egon_etrago_load_timeseries",
            foreign_column="load_id",
            reference_dataset="grid.egon_etrago_load",
            reference_column="load_id"
        )

        # Simulate DB result: 25 orphaned references (data cleanup needed)
        mock_db_row = {
            "total_non_null_references": 5025,
            "valid_references": 5000,
            "orphaned_references": 25
        }

        result = rule.postprocess(mock_db_row, None)

        # Should fail - orphaned references detected
        assert result.success is False
        assert result.message == "25 orphaned references found in load_id (out of 5025 total non-null references)"
        assert result.observed == 25.0
        assert result.rule_id == "load_timeseries_integrity"
        assert result.column == "load_id"

    def test_postprocess_none_values_handling(self):
        """Test handling of None values in database result"""
        rule = ReferentialIntegrityValidation(
            "test_rule",
            "test_task",
            "test.table",
            reference_dataset="test.reference"
        )

        mock_db_row = {
            "total_non_null_references": None,
            "valid_references": None,
            "orphaned_references": None
        }

        result = rule.postprocess(mock_db_row, None)

        # Should handle None values gracefully
        assert result.success is True  # 0 orphaned_references = success
        assert result.message == "All 0 references in id have valid matches in test.reference.id"
        assert result.observed == 0.0

    def test_with_mock_data_success_bus_references(self):
        """Test with realistic mock data: all loads reference valid buses"""
        rule = ReferentialIntegrityValidation(
            "load_bus_integrity",
            "grid_validation",
            "grid.egon_etrago_load",
            foreign_column="bus",
            reference_dataset="grid.egon_etrago_bus",
            reference_column="bus_id"
        )

        # Simulate DB result: all 12000 loads connected to valid buses
        mock_db_row = {
            "total_non_null_references": 12000,
            "valid_references": 12000,
            "orphaned_references": 0
        }

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - all loads properly connected
        assert result.success is True
        assert result.message == "All 12000 references in bus have valid matches in grid.egon_etrago_bus.bus_id"
        assert result.rule_id == "load_bus_integrity"
        assert result.task == "grid_validation"
        assert result.dataset == "grid.egon_etrago_load"
        assert result.column == "bus"

    def test_with_mock_data_failure_missing_buses(self):
        """Test with realistic mock data: some loads reference deleted buses"""
        rule = ReferentialIntegrityValidation(
            "generator_bus_integrity",
            "grid_validation",
            "grid.egon_etrago_generator",
            foreign_column="bus",
            reference_dataset="grid.egon_etrago_bus",
            reference_column="bus_id"
        )

        # Simulate DB result: grid topology changes left orphaned generators
        mock_db_row = {
            "total_non_null_references": 8550,
            "valid_references": 8500,
            "orphaned_references": 50
        }

        result = rule.postprocess(mock_db_row, None)

        # Should fail - topology inconsistency detected
        assert result.success is False
        assert result.message == "50 orphaned references found in bus (out of 8550 total non-null references)"
        assert result.observed == 50.0
        assert result.rule_id == "generator_bus_integrity"
        assert result.dataset == "grid.egon_etrago_generator"
        assert result.column == "bus"

    def test_with_mock_data_success_demand_region_references(self):
        """Test with realistic mock data: demand data references valid regions"""
        rule = ReferentialIntegrityValidation(
            "demand_region_integrity",
            "demand_validation",
            "demand.egon_demandregio_hh",
            foreign_column="nuts3",
            reference_dataset="boundaries.vg250_krs",
            reference_column="nuts"
        )

        # Simulate DB result: all household demand entries reference valid districts
        mock_db_row = {
            "total_non_null_references": 1203,  # 401 districts * 3 scenarios
            "valid_references": 1203,
            "orphaned_references": 0
        }

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - complete regional coverage
        assert result.success is True
        assert result.message == "All 1203 references in nuts3 have valid matches in boundaries.vg250_krs.nuts"
        assert result.rule_id == "demand_region_integrity"
        assert result.task == "demand_validation"
        assert result.column == "nuts3"

    def test_with_mock_data_failure_outdated_region_codes(self):
        """Test with realistic mock data: demand uses outdated region codes"""
        rule = ReferentialIntegrityValidation(
            "cts_region_integrity",
            "demand_validation",
            "demand.egon_demandregio_cts",
            foreign_column="nuts3",
            reference_dataset="boundaries.vg250_krs",
            reference_column="nuts"
        )

        # Simulate DB result: territorial reform created orphaned region references
        mock_db_row = {
            "total_non_null_references": 1208,
            "valid_references": 1203,
            "orphaned_references": 5  # Old region codes
        }

        result = rule.postprocess(mock_db_row, None)

        # Should fail - outdated region codes detected
        assert result.success is False
        assert result.message == "5 orphaned references found in nuts3 (out of 1208 total non-null references)"
        assert result.observed == 5.0
        assert result.rule_id == "cts_region_integrity"
        assert result.column == "nuts3"