import pytest
from egon_validation.rules.formal.null_check import NotNullAndNotNaN
from egon_validation.rules.base import Severity


class TestNotNullAndNotNaN:
    def test_sql_generation_default_column(self):
        rule = NotNullAndNotNaN("test_rule", "test_task", "test.table")
        sql = rule.sql(None)

        expected = "SELECT COUNT(*) AS n_bad FROM test.table WHERE (None IS NULL OR None <> None)"
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

    def test_with_mock_data_success_no_nulls(self):
        """Test with realistic mock data: clean dataset without NULL/NaN values"""
        # Mock a dataset like demand.egon_demandregio_hh with clean demand values
        rule = NotNullAndNotNaN(
            "test_null_check",
            "data_quality",
            "demand.egon_demandregio_hh",
            column="demand"
        )

        # Simulate DB result: 0 rows with NULL or NaN values found
        mock_db_row = {"n_bad": 0}

        result = rule.postprocess(mock_db_row, None)

        # Should succeed - clean data
        assert result.success is True
        assert result.message == "0 offending rows (NULL or NaN)"
        assert result.rule_id == "test_null_check"
        assert result.task == "data_quality"
        assert result.dataset == "demand.egon_demandregio_hh"
        assert result.column == "demand"
        assert result.severity == Severity.WARNING

    def test_with_mock_data_failure_has_nulls(self):
        """Test with realistic mock data: dataset with NULL/NaN values"""
        # Mock a corrupted dataset with missing demand values
        rule = NotNullAndNotNaN(
            "demand_null_check",
            "data_quality",
            "demand.egon_demandregio_hh",
            column="demand"
        )

        # Simulate DB result: 127 rows have NULL or NaN demand values
        mock_db_row = {"n_bad": 127}

        result = rule.postprocess(mock_db_row, None)

        # Should fail - corrupted data detected
        assert result.success is False
        assert result.message == "127 offending rows (NULL or NaN)"
        assert result.rule_id == "demand_null_check"
        assert result.dataset == "demand.egon_demandregio_hh"
        assert result.column == "demand"