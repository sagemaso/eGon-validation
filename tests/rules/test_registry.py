import pytest
from egon_validation.rules.registry import (
    register,
    register_map,
    rules_for,
    list_registered,
    _REGISTRY,
)
from egon_validation.rules.base import Rule, SqlRule, RuleResult, Severity


class MockRule(Rule):
    """Mock rule for testing registry - returns success=True as placeholder."""
    def evaluate(self, engine, ctx):
        return self.create_result(success=True)


class MockSqlRule(SqlRule):
    """Mock SQL rule for testing registry - returns success=True as placeholder."""
    def sql(self, ctx):
        return "SELECT 1"

    def postprocess(self, row, ctx):
        return self.create_result(success=True)


class TestRegistry:
    def setup_method(self):
        _REGISTRY.clear()

    def teardown_method(self):
        _REGISTRY.clear()

    def test_register_decorator_basic(self):
        @register(task="test_task", table="test.table")
        class TestValidationRule(MockRule):
            pass

        assert len(_REGISTRY) == 1
        rule_id, task, table, rule_cls, params = _REGISTRY[0]

        assert rule_id == "TestValidationRule"
        assert task == "test_task"
        assert table == "test.table"
        assert rule_cls == TestValidationRule
        assert params == {}

    def test_register_decorator_with_rule_id(self):
        @register(task="test_task", table="test.table", rule_id="CUSTOM_RULE_ID")
        class AnotherRule(MockRule):
            pass

        rule_id, _, _, _, _ = _REGISTRY[0]
        assert rule_id == "CUSTOM_RULE_ID"

    def test_register_decorator_with_params(self):
        @register(
            task="test_task",
            table="test.table",
            column="test_col",
            threshold=0.5,
        )
        class ParameterizedRule(MockRule):
            pass

        rule_id, task, table, rule_cls, params = _REGISTRY[0]

        assert params["column"] == "test_col"
        assert params["threshold"] == 0.5

    def test_register_multiple_rules(self):
        @register(task="task1", table="table1")
        class Rule1(MockRule):
            pass

        @register(task="task2", table="table2")
        class Rule2(MockRule):
            pass

        assert len(_REGISTRY) == 2
        assert _REGISTRY[0][0] == "Rule1"
        assert _REGISTRY[1][0] == "Rule2"

    def test_register_map_single_table(self):
        register_map(
            task="test_task",
            rule_cls=MockSqlRule,
            rule_id="MAP_RULE",
            tables_params={"schema1.table1": {"column": "col1", "threshold": 10}},
        )

        assert len(_REGISTRY) == 1
        rule_id, task, table, rule_cls, params = _REGISTRY[0]

        assert rule_id == "MAP_RULE"
        assert task == "test_task"
        assert table == "schema1.table1"
        assert rule_cls == MockSqlRule
        assert params["column"] == "col1"
        assert params["threshold"] == 10

    def test_register_map_multiple_tables(self):
        register_map(
            task="test_task",
            rule_cls=MockSqlRule,
            tables_params={
                "schema1.table1": {"column": "col1", "threshold": 10},
                "schema2.table2": {"column": "col2", "threshold": 20},
            },
        )

        assert len(_REGISTRY) == 2

        rule_id1, task1, table1, rule_cls1, params1 = _REGISTRY[0]
        assert table1 == "schema1.table1"
        assert params1["column"] == "col1"
        assert params1["threshold"] == 10

        rule_id2, task2, table2, rule_cls2, params2 = _REGISTRY[1]
        assert table2 == "schema2.table2"
        assert params2["column"] == "col2"
        assert params2["threshold"] == 20

    def test_register_map_default_rule_id(self):
        register_map(
            task="test_task",
            rule_cls=MockSqlRule,
            tables_params={"test.table": {}}
        )

        rule_id, _, _, _, _ = _REGISTRY[0]
        assert rule_id == "MockSqlRule"

    def test_rules_for_task(self):
        @register(task="task1", table="table1", param1="value1")
        class Rule1(MockRule):
            pass

        @register(task="task2", table="table2", param2="value2")
        class Rule2(MockRule):
            pass

        @register(task="task1", table="table3", param3="value3")
        class Rule3(MockRule):
            pass

        rules_task1 = list(rules_for("task1"))
        assert len(rules_task1) == 2

        rule_names = [rule.rule_id for rule in rules_task1]
        assert "Rule1" in rule_names
        assert "Rule3" in rule_names

        rule1 = next(rule for rule in rules_task1 if rule.rule_id == "Rule1")
        assert rule1.params["param1"] == "value1"

    def test_rules_for_nonexistent_task(self):
        @register(task="existing_task", table="table1")
        class SomeRule(MockRule):
            pass

        rules = list(rules_for("nonexistent_task"))
        assert len(rules) == 0

    def test_rules_for_empty_registry(self):
        rules = list(rules_for("any_task"))
        assert len(rules) == 0

    def test_list_registered_empty(self):
        registered = list_registered()
        assert registered == []

    def test_list_registered_with_rules(self):
        @register(
            task="task1",
            table="schema.table1",
            rule_id="RULE1",
            column="col1",
        )
        class Rule1(MockRule):
            pass

        @register(task="task2", table="schema.table2")
        class Rule2(MockRule):
            pass

        registered = list_registered()
        assert len(registered) == 2

        rule1_info = next(r for r in registered if r["rule_id"] == "RULE1")
        assert rule1_info["task"] == "task1"
        assert rule1_info["table"] == "schema.table1"
        assert rule1_info["params"]["column"] == "col1"

        rule2_info = next(r for r in registered if r["rule_id"] == "Rule2")
        assert rule2_info["task"] == "task2"
        assert rule2_info["table"] == "schema.table2"

    def test_register_preserves_original_class(self):
        @register(task="test_task", table="test.table")
        class OriginalRule(MockRule):
            custom_method = lambda self: "test"

        assert hasattr(OriginalRule, "custom_method")
        instance = OriginalRule(rule_id="test_rule", table="test.table")
        assert instance.custom_method() == "test"

    def test_register_map_preserves_params(self):
        original_params = {"column": "col1", "threshold": 10}

        register_map(
            task="test_task",
            rule_cls=MockSqlRule,
            tables_params={"test.table": original_params},
        )

        assert original_params == {"column": "col1", "threshold": 10}

        _, _, _, _, stored_params = _REGISTRY[0]
        assert stored_params == original_params
        assert stored_params is not original_params

    def test_rules_for_instantiation(self):
        @register(
            task="test_task", table="schema.table", param="value"
        )
        class TestRuleForInstantiation(MockRule):
            pass

        rules = list(rules_for("test_task"))
        rule = rules[0]

        assert isinstance(rule, TestRuleForInstantiation)
        assert rule.rule_id == "TestRuleForInstantiation"
        assert rule.task == "test_task"
        assert rule.table == "schema.table"
        assert rule.schema == "schema"
        assert rule.table_name == "table"
        assert rule.params["param"] == "value"

    def test_registry_thread_safety_assumption(self):
        initial_count = len(_REGISTRY)

        @register(task="concurrent_task", table="test.table")
        class ConcurrentRule(MockRule):
            pass

        assert len(_REGISTRY) == initial_count + 1
        assert _REGISTRY[-1][0] == "ConcurrentRule"