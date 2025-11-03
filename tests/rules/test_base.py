import pytest
from unittest.mock import Mock, patch
from egon_validation.rules.base import Rule, SqlRule, RuleResult, Severity


class TestRuleResult:
    def test_rule_result_creation(self):
        result = RuleResult(
            rule_id="test_rule",
            task="test_task",
            dataset="test.table",
            success=True,
            message="Test passed",
        )
        assert result.rule_id == "test_rule"
        assert result.task == "test_task"
        assert result.dataset == "test.table"
        assert result.success is True
        assert result.message == "Test passed"
        assert result.severity == Severity.WARNING  # default
        assert result.observed is None
        assert result.expected is None

    def test_rule_result_to_dict(self):
        result = RuleResult(
            rule_id="test_rule",
            task="test_task",
            dataset="test.table",
            success=False,
            message="Test failed",
            severity=Severity.ERROR,
            observed=10.5,
            expected=5.0,
        )

        result_dict = result.to_dict()
        assert result_dict["rule_id"] == "test_rule"
        assert result_dict["success"] is False
        assert result_dict["severity"] == "ERROR"
        assert result_dict["observed"] == 10.5
        assert result_dict["expected"] == 5.0

    def test_rule_result_enum_conversion(self):
        result = RuleResult(
            rule_id="test",
            task="test",
            dataset="test.table",
            success=True,
            severity=Severity.INFO,
        )
        result_dict = result.to_dict()
        assert result_dict["severity"] == "INFO"


class TestRule:
    def test_rule_initialization(self):
        rule = Rule("test_rule", "test_task", "schema.table", param1="value1")

        assert rule.rule_id == "test_rule"
        assert rule.task == "test_task"
        assert rule.dataset == "schema.table"
        assert rule.params == {"param1": "value1"}
        assert rule.schema == "schema"
        assert rule.table == "table"

    def test_rule_initialization_no_schema(self):
        rule = Rule("test_rule", "test_task", "table_only")

        assert rule.dataset == "table_only"
        assert rule.schema is None
        assert rule.table == "table_only"


class TestSqlRule:
    def test_sql_rule_inheritance(self):
        class TestSqlRule(SqlRule):
            def sql(self, ctx):
                return "SELECT 1"

            def postprocess(self, row, ctx):
                return RuleResult(self.rule_id, self.task, self.dataset, True)

        rule = TestSqlRule("test_rule", "test_task", "test.table")
        assert isinstance(rule, SqlRule)
        assert isinstance(rule, Rule)

    def test_sql_not_implemented(self):
        rule = SqlRule("test_rule", "test_task", "test.table")

        with pytest.raises(NotImplementedError):
            rule.sql(None)

    def test_postprocess_not_implemented(self):
        rule = SqlRule("test_rule", "test_task", "test.table")

        with pytest.raises(NotImplementedError):
            rule.postprocess({}, None)

    @patch("egon_validation.db.fetch_one")
    def test_check_table_empty_with_data(
        self, mock_fetch_one, mock_engine, mock_context
    ):
        mock_fetch_one.return_value = {"total_count": 100}

        rule = SqlRule("test_rule", "test_task", "test.table")
        result = rule._check_table_empty(mock_engine, mock_context)

        assert result is None  # Table has data, continue validation
        mock_fetch_one.assert_called_once()

    @patch("egon_validation.db.fetch_one")
    def test_check_table_empty_no_data(self, mock_fetch_one, mock_engine, mock_context):
        mock_fetch_one.return_value = {"total_count": 0}

        rule = SqlRule("test_rule", "test_task", "test.table")
        result = rule._check_table_empty(mock_engine, mock_context)

        assert result is not None
        assert result.success is False
        assert result.observed == 0
        assert result.expected == ">0"
        assert "EMPTY TABLE" in result.message
        assert result.severity == Severity.INFO

    @patch("egon_validation.db.fetch_one")
    def test_check_empty_table(self, mock_fetch_one, mock_engine, mock_context):
        mock_fetch_one.return_value = {"total_count": 0}

        rule = SqlRule(
            "test_rule",
            "test_task",
            "test.table",
            scenario_col="scenario",
            scenario="test_scenario",
        )
        result = rule._check_table_empty(mock_engine, mock_context)

        assert result is not None
        assert result.success is False
        assert "EMPTY TABLE" in result.message
        assert result.observed == 0
        assert result.expected == ">0"
        # Check that fetch_one was called with just the engine and query (no scenario params)
        mock_fetch_one.assert_called_once()

    @patch("egon_validation.db.fetch_one")
    def test_check_table_empty_exception(
        self, mock_fetch_one, mock_engine, mock_context
    ):
        mock_fetch_one.side_effect = Exception("Database error")

        rule = SqlRule("test_rule", "test_task", "test.table")
        result = rule._check_table_empty(mock_engine, mock_context)

        # Should return None when exception occurs, let main query handle it
        assert result is None


class TestSeverity:
    def test_severity_enum_values(self):
        assert Severity.INFO.value == "INFO"
        assert Severity.WARNING.value == "WARNING"
        assert Severity.ERROR.value == "ERROR"

    def test_severity_enum_comparison(self):
        assert Severity.INFO == Severity.INFO
        assert Severity.WARNING != Severity.ERROR
