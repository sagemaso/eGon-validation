import pytest
from egon_validation.rules.formal.data_type_check import DataTypeValidation
from egon_validation.rules.base import Severity


class TestDataTypeValidation:
    def test_sql_generation_with_schema(self):
        rule = DataTypeValidation(
            rule_id="test_rule",
            table="schema.table",
            column_types={"year": "integer"}
        )
        sql = rule.sql(None)

        assert "table_schema = 'schema'" in sql
        assert "table_name = 'table'" in sql
        assert "column_name IN ('year')" in sql

    def test_sql_generation_multiple_columns(self):
        rule = DataTypeValidation(
            rule_id="test_rule",
            table="schema.table",
            column_types={"year": "integer", "name": "text"}
        )
        sql = rule.sql(None)

        assert "json_agg" in sql
        assert "table_schema = 'schema'" in sql

    def test_sql_generation_without_schema(self):
        """Test that tables without schema raise an error"""
        rule = DataTypeValidation(
            rule_id="test_rule",
            table="table_only",
            column_types={"year": "integer"}
        )
        with pytest.raises(ValueError, match="must include schema"):
            rule.sql(None)

    def test_postprocess_no_columns_info(self):
        rule = DataTypeValidation(
            rule_id="test_rule",
            table="test.table",
            column_types={"year": "integer"}
        )

        result = rule.postprocess({"columns_info": None}, None)

        assert result.success is False
        assert "No column information found" in result.message
        assert result.severity == Severity.ERROR

    def test_postprocess_all_valid(self):
        rule = DataTypeValidation(
            rule_id="test_rule",
            table="test.table",
            column_types={"year": "integer", "name": "text"}
        )

        columns_info = [
            {"column_name": "year", "data_type": "integer", "udt_name": "int4"},
            {"column_name": "name", "data_type": "text", "udt_name": "text"},
        ]
        row = {"columns_info": columns_info}

        result = rule.postprocess(row, None)

        assert result.success is True
        assert result.message == "All column types valid"
        assert result.observed == 0

    def test_postprocess_type_mismatch(self):
        rule = DataTypeValidation(
            rule_id="test_rule",
            table="test.table",
            column_types={"year": "integer", "name": "text"}
        )

        columns_info = [
            {"column_name": "year", "data_type": "text", "udt_name": "text"},
            {"column_name": "name", "data_type": "text", "udt_name": "text"},
        ]
        row = {"columns_info": columns_info}

        result = rule.postprocess(row, None)

        assert result.success is False
        assert "year: got 'text'" in result.message
        assert result.observed == 1

    def test_postprocess_missing_column(self):
        rule = DataTypeValidation(
            rule_id="test_rule",
            table="test.table",
            column_types={"year": "integer", "missing": "text"}
        )

        columns_info = [
            {"column_name": "year", "data_type": "integer", "udt_name": "int4"}
        ]
        row = {"columns_info": columns_info}

        result = rule.postprocess(row, None)

        assert result.success is False
        assert "missing: column not found" in result.message

    def test_type_mappings(self):
        """Test that PostgreSQL type mappings work correctly"""
        rule = DataTypeValidation(
            rule_id="test_rule",
            table="test.table",
            column_types={"id": "integer"}
        )

        # bigint should match integer type family
        columns_info = [
            {"column_name": "id", "data_type": "bigint", "udt_name": "int8"}
        ]
        row = {"columns_info": columns_info}

        result = rule.postprocess(row, None)
        assert result.success is True

    def test_text_type_mappings(self):
        """Test that text type variations match"""
        rule = DataTypeValidation(
            rule_id="test_rule",
            table="test.table",
            column_types={"name": "text"}
        )

        columns_info = [
            {"column_name": "name", "data_type": "character varying", "udt_name": "varchar"}
        ]
        row = {"columns_info": columns_info}

        result = rule.postprocess(row, None)
        assert result.success is True

    def test_with_mock_data_success_all_types_correct(self):
        """Test with realistic mock data: all columns have correct types"""
        rule = DataTypeValidation(
            rule_id="grid_schema_check",
            table="grid.egon_etrago_load",
            column_types={
                "bus_id": "integer",
                "carrier": "text",
                "scn_name": "text",
                "p_set": "numeric"
            }
        )

        mock_columns_info = [
            {"column_name": "bus_id", "data_type": "integer", "udt_name": "int4"},
            {"column_name": "carrier", "data_type": "text", "udt_name": "text"},
            {"column_name": "scn_name", "data_type": "character varying", "udt_name": "varchar"},
            {"column_name": "p_set", "data_type": "numeric", "udt_name": "numeric"}
        ]
        mock_db_row = {"columns_info": mock_columns_info}

        result = rule.postprocess(mock_db_row, None)

        assert result.success is True
        assert result.message == "All column types valid"
        assert result.observed == 0
        assert result.rule_id == "grid_schema_check"
        assert result.table == "grid.egon_etrago_load"

    def test_with_mock_data_failure_wrong_types(self):
        """Test with realistic mock data: some columns have wrong types"""
        rule = DataTypeValidation(
            rule_id="demand_schema_check",
            table="demand.egon_demandregio_hh",
            column_types={
                "year": "integer",
                "nuts3": "text",
                "demand": "numeric",
                "sector": "text"
            }
        )

        mock_columns_info = [
            {"column_name": "year", "data_type": "text", "udt_name": "text"},
            {"column_name": "nuts3", "data_type": "text", "udt_name": "text"},
            {"column_name": "demand", "data_type": "text", "udt_name": "text"},
            {"column_name": "sector", "data_type": "text", "udt_name": "text"}
        ]
        mock_db_row = {"columns_info": mock_columns_info}

        result = rule.postprocess(mock_db_row, None)

        assert result.success is False
        assert "year: got 'text'" in result.message
        assert "demand: got 'text'" in result.message
        assert result.observed == 2
        assert result.rule_id == "demand_schema_check"