from egon_validation.rules.formal.null_check import NotNullAndNotNaNValidation
from egon_validation.rules.base import Severity


class TestNotNullAndNotNaNValidation:
    def test_sql_generation_single_column(self):
        rule = NotNullAndNotNaNValidation(
            rule_id="test_rule", table="test.table", columns=["demand"]
        )
        sql = rule.get_query(None)

        assert "demand" in sql
        assert "IS NULL OR" in sql
        assert "json_agg" in sql

    def test_sql_generation_multiple_columns(self):
        rule = NotNullAndNotNaNValidation(
            rule_id="test_rule", table="test.table", columns=["demand", "year"]
        )
        sql = rule.get_query(None)

        assert "demand" in sql
        assert "year" in sql

    def test_sql_generation_empty_columns(self):
        rule = NotNullAndNotNaNValidation(
            rule_id="test_rule", table="test.table", columns=[]
        )
        sql = rule.get_query(None)

        assert "NULL as columns_info" in sql

    def test_postprocess_success(self):
        rule = NotNullAndNotNaNValidation(
            rule_id="test_rule", table="test.table", columns=["demand"]
        )
        row = {"columns_info": [{"column_name": "demand", "null_nan_count": 0}]}

        result = rule.postprocess(row, None)

        assert result.success is True
        assert result.severity == Severity.INFO

    def test_postprocess_failure(self):
        rule = NotNullAndNotNaNValidation(
            rule_id="test_rule", table="test.table", columns=["demand"]
        )
        row = {"columns_info": [{"column_name": "demand", "null_nan_count": 5}]}

        result = rule.postprocess(row, None)

        assert result.success is False
        assert "5" in result.message

    def test_postprocess_no_columns_info(self):
        rule = NotNullAndNotNaNValidation(
            rule_id="test_rule", table="test.table", columns=["demand"]
        )
        row = {"columns_info": None}

        result = rule.postprocess(row, None)

        assert result.success is False
        assert result.severity == Severity.ERROR

    def test_with_mock_data_success_no_nulls(self):
        """Test with realistic mock data: clean dataset without NULL/NaN values"""
        rule = NotNullAndNotNaNValidation(
            rule_id="test_null_check",
            table="demand.egon_demandregio_hh",
            columns=["demand", "year"],
        )

        mock_db_row = {
            "columns_info": [
                {"column_name": "demand", "null_nan_count": 0},
                {"column_name": "year", "null_nan_count": 0},
            ]
        }

        result = rule.postprocess(mock_db_row, None)

        assert result.success is True
        assert result.rule_id == "test_null_check"
        assert result.table == "demand.egon_demandregio_hh"

    def test_with_mock_data_failure_has_nulls(self):
        """Test with realistic mock data: dataset with NULL/NaN values"""
        rule = NotNullAndNotNaNValidation(
            rule_id="demand_null_check",
            table="demand.egon_demandregio_hh",
            columns=["demand"],
        )

        mock_db_row = {
            "columns_info": [{"column_name": "demand", "null_nan_count": 127}]
        }

        result = rule.postprocess(mock_db_row, None)

        assert result.success is False
        assert "127" in result.message
        assert result.rule_id == "demand_null_check"
