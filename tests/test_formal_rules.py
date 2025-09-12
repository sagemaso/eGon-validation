import pytest
from unittest.mock import Mock, patch
from egon_validation.rules.formal.null_check import NotNullAndNotNaN
from egon_validation.rules.formal.data_type_check import (
    DataTypeValidation,
    MultipleColumnsDataTypeValidation,
)
from egon_validation.rules.formal.range_check import Range
from egon_validation.rules.formal.value_set_check import ValueSetValidation
from egon_validation.rules.base import RuleResult, Severity


class TestNotNullAndNotNaN:
    def test_sql_generation_default_column(self):
        rule = NotNullAndNotNaN("test_rule", "test_task", "test.table")
        sql = rule.sql(None)

        expected = "SELECT COUNT(*) AS n_bad FROM test.table WHERE (value IS NULL OR value <> value)"
        assert sql == expected

    def test_sql_generation_custom_column(self):
        rule = NotNullAndNotNaN("test_rule", "test_task", "test.table", column="demand")
        sql = rule.sql(None)

        expected = "SELECT COUNT(*) AS n_bad FROM test.table WHERE (demand IS NULL OR demand <> demand)"
        assert sql == expected

    def test_postprocess_success(self):
        rule = NotNullAndNotNaN("test_rule", "test_task", "test.table", column="demand")
        row = {"n_bad": 0}

        result = rule.postprocess(row, None)

        assert result.success is True
        assert result.message == "0 offending rows (NULL or NaN)"
        assert result.severity == Severity.WARNING
        assert result.column == "demand"

    def test_postprocess_failure(self):
        rule = NotNullAndNotNaN("test_rule", "test_task", "test.table")
        row = {"n_bad": 5}

        result = rule.postprocess(row, None)

        assert result.success is False
        assert result.message == "5 offending rows (NULL or NaN)"

    def test_postprocess_none_value(self):
        rule = NotNullAndNotNaN("test_rule", "test_task", "test.table")
        row = {"n_bad": None}

        result = rule.postprocess(row, None)

        assert result.success is True
        assert result.message == "0 offending rows (NULL or NaN)"


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


class TestRange:
    def test_sql_generation_default_params(self):
        # Use actual config values: LOAD_PROFILE_MAX_VALUE = 1.2
        rule = Range("test_rule", "test_task", "test.table")
        sql = rule.sql(None)

        expected = "SELECT COUNT(*) AS n_bad FROM test.table WHERE (value < 0.0 OR value > 1.2)"
        assert sql == expected

    def test_sql_generation_custom_params(self):
        rule = Range(
            "test_rule",
            "test_task",
            "test.table",
            column="price",
            min_val=10.5,
            max_val=100.0,
        )
        sql = rule.sql(None)

        expected = "SELECT COUNT(*) AS n_bad FROM test.table WHERE (price < 10.5 OR price > 100.0)"
        assert sql == expected

    def test_postprocess_all_in_range(self):
        rule = Range("test_rule", "test_task", "test.table", column="value")
        row = {"n_bad": 0}

        result = rule.postprocess(row, None)

        assert result.success is True
        assert result.message == "0 rows outside range"
        assert result.severity == Severity.WARNING

    def test_postprocess_some_out_of_range(self):
        rule = Range("test_rule", "test_task", "test.table", column="value")
        row = {"n_bad": 3}

        result = rule.postprocess(row, None)

        assert result.success is False
        assert result.message == "3 rows outside range"
        assert result.column == "value"


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
