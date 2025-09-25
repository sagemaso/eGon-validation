import pytest
from egon_validation.rules.formal.data_type_check import (
    DataTypeValidation,
    MultipleColumnsDataTypeValidation,
)
from egon_validation.rules.base import Severity


class TestDataTypeValidation:
    def test_sql_generation_with_schema(self):
        rule = DataTypeValidation(
            "test_rule",
            "test_task",
            "schema.table",
            column="year",
            expected_type="integer",
        )
        sql = rule.sql(None)

        assert "table_schema = 'schema'" in sql
        assert "table_name = 'table'" in sql
        assert "column_name = 'year'" in sql

    def test_sql_generation_without_schema(self):
        rule = DataTypeValidation(
            "test_rule",
            "test_task",
            "table_only",
            column="year",
            expected_type="integer",
        )
        sql = rule.sql(None)

        assert "table_schema = 'public'" in sql
        assert "table_name = 'table_only'" in sql

    def test_postprocess_column_not_found(self):
        rule = DataTypeValidation("test_rule", "test_task", "test.table")

        result = rule.postprocess(None, None)

        assert result.success is False
        assert result.message == "Column not found"
        assert result.severity == Severity.ERROR

    def test_postprocess_type_match(self):
        rule = DataTypeValidation(
            "test_rule", "test_task", "test.table", column="id", expected_type="integer"
        )
        row = {"column_name": "id", "data_type": "integer", "udt_name": "int4"}

        result = rule.postprocess(row, None)

        assert result.success is True
        assert "Column 'id' has type 'integer'" in result.message

    def test_postprocess_type_mismatch(self):
        rule = DataTypeValidation(
            "test_rule",
            "test_task",
            "test.table",
            column="name",
            expected_type="integer",
        )
        row = {"column_name": "name", "data_type": "text", "udt_name": "text"}

        result = rule.postprocess(row, None)

        assert result.success is False
        assert "has type 'text'" in result.message
        assert "expected: integer" in result.message

    def test_type_mappings(self):
        rule = DataTypeValidation("test_rule", "test_task", "test.table")

        # Test integer types
        row = {"column_name": "id", "data_type": "bigint", "udt_name": "int8"}
        rule.params = {"expected_type": "integer"}
        result = rule.postprocess(row, None)
        assert result.success is True

        # Test text types
        row = {
            "column_name": "name",
            "data_type": "character varying",
            "udt_name": "varchar",
        }
        rule.params = {"expected_type": "text"}
        result = rule.postprocess(row, None)
        assert result.success is True

    def test_with_mock_data_success_correct_type(self):
        """Test with realistic mock data: column has expected data type"""
        # Mock validating year column in demand table
        rule = DataTypeValidation(
            "year_type_check",
            "data_validation",
            "demand.egon_demandregio_hh",
            column="year",
            expected_type="integer"
        )

        # Simulate DB result: year column is correctly integer type
        mock_db_row = {
            "column_name": "year",
            "data_type": "integer",
            "udt_name": "int4"
        }

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - correct data type
        assert result.success is True
        assert "Column 'year' has type 'integer'" in result.message
        assert result.rule_id == "year_type_check"
        assert result.task == "data_validation"
        assert result.dataset == "demand.egon_demandregio_hh"
        assert result.column == "year"

    def test_with_mock_data_failure_wrong_type(self):
        """Test with realistic mock data: column has wrong data type"""
        # Mock validating bus_id that should be integer but is text
        rule = DataTypeValidation(
            "bus_id_type_check",
            "data_validation",
            "grid.egon_etrago_load",
            column="bus_id",
            expected_type="integer"
        )

        # Simulate DB result: bus_id column is incorrectly text type
        mock_db_row = {
            "column_name": "bus_id",
            "data_type": "text",
            "udt_name": "text"
        }

        result = rule.postprocess(mock_db_row, None)

        # Should fail - wrong data type detected
        assert result.success is False
        assert "has type 'text'" in result.message
        assert "expected: integer" in result.message
        assert result.rule_id == "bus_id_type_check"
        assert result.dataset == "grid.egon_etrago_load"
        assert result.column == "bus_id"
        assert result.severity == Severity.WARNING


class TestMultipleColumnsDataTypeValidation:
    def test_sql_generation(self):
        rule = MultipleColumnsDataTypeValidation(
            "test_rule",
            "test_task",
            "schema.table",
            column_types={"year": "integer", "name": "text"},
        )
        sql = rule.sql(None)

        assert "json_agg" in sql
        assert "table_schema = 'schema'" in sql
        assert "column_name IN ('year', 'name')" in sql

    def test_postprocess_no_columns_info(self):
        rule = MultipleColumnsDataTypeValidation(
            "test_rule", "test_task", "test.table", column_types={"year": "integer"}
        )

        result = rule.postprocess({"columns_info": None}, None)

        assert result.success is False
        assert result.message == "No column information found"
        assert result.severity == Severity.ERROR

    def test_postprocess_all_valid(self):
        rule = MultipleColumnsDataTypeValidation(
            "test_rule",
            "test_task",
            "test.table",
            column_types={"year": "integer", "name": "text"},
        )

        columns_info = [
            {"column_name": "year", "data_type": "integer", "udt_name": "int4"},
            {"column_name": "name", "data_type": "text", "udt_name": "text"},
        ]
        row = {"columns_info": columns_info}

        result = rule.postprocess(row, None)

        assert result.success is True
        assert result.message == "All column types valid"
        assert result.observed == 0.0

    def test_postprocess_type_mismatch(self):
        rule = MultipleColumnsDataTypeValidation(
            "test_rule",
            "test_task",
            "test.table",
            column_types={"year": "integer", "name": "text"},
        )

        columns_info = [
            {"column_name": "year", "data_type": "text", "udt_name": "text"},
            {"column_name": "name", "data_type": "text", "udt_name": "text"},
        ]
        row = {"columns_info": columns_info}

        result = rule.postprocess(row, None)

        assert result.success is False
        assert "year: got 'text'" in result.message
        assert result.observed == 1.0

    def test_postprocess_missing_column(self):
        rule = MultipleColumnsDataTypeValidation(
            "test_rule",
            "test_task",
            "test.table",
            column_types={"year": "integer", "missing": "text"},
        )

        columns_info = [
            {"column_name": "year", "data_type": "integer", "udt_name": "int4"}
        ]
        row = {"columns_info": columns_info}

        result = rule.postprocess(row, None)

        assert result.success is False
        assert "missing: column not found" in result.message

    def test_with_mock_data_success_all_types_correct(self):
        """Test with realistic mock data: all columns have correct types"""
        # Mock validating multiple columns in grid table
        rule = MultipleColumnsDataTypeValidation(
            "grid_schema_check",
            "schema_validation",
            "grid.egon_etrago_load",
            column_types={
                "bus_id": "integer",
                "carrier": "text",
                "scn_name": "text",
                "p_set": "numeric"
            }
        )

        # Simulate DB result: all columns have correct types
        mock_columns_info = [
            {"column_name": "bus_id", "data_type": "integer", "udt_name": "int4"},
            {"column_name": "carrier", "data_type": "text", "udt_name": "text"},
            {"column_name": "scn_name", "data_type": "character varying", "udt_name": "varchar"},
            {"column_name": "p_set", "data_type": "numeric", "udt_name": "numeric"}
        ]
        mock_db_row = {"columns_info": mock_columns_info}

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - all types correct
        assert result.success is True
        assert result.message == "All column types valid"
        assert result.observed == 0.0
        assert result.rule_id == "grid_schema_check"
        assert result.task == "schema_validation"
        assert result.dataset == "grid.egon_etrago_load"

    def test_with_mock_data_failure_wrong_types(self):
        """Test with realistic mock data: some columns have wrong types"""
        # Mock validating demand table with type mismatches
        rule = MultipleColumnsDataTypeValidation(
            "demand_schema_check",
            "schema_validation",
            "demand.egon_demandregio_hh",
            column_types={
                "year": "integer",
                "nuts3": "text",
                "demand": "numeric",
                "sector": "text"
            }
        )

        # Simulate DB result: year and demand have wrong types
        mock_columns_info = [
            {"column_name": "year", "data_type": "text", "udt_name": "text"},  # Should be integer
            {"column_name": "nuts3", "data_type": "text", "udt_name": "text"},  # Correct
            {"column_name": "demand", "data_type": "text", "udt_name": "text"},  # Should be numeric
            {"column_name": "sector", "data_type": "text", "udt_name": "text"}  # Correct
        ]
        mock_db_row = {"columns_info": mock_columns_info}

        result = rule.postprocess(mock_db_row, None)

        # Should fail - type mismatches detected
        assert result.success is False
        assert "year: got 'text'" in result.message
        assert "demand: got 'text'" in result.message
        assert result.observed == 2.0  # 2 columns with wrong types
        assert result.rule_id == "demand_schema_check"
        assert result.dataset == "demand.egon_demandregio_hh"
        assert result.severity == Severity.WARNING