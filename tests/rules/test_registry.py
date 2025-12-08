import pytest
from unittest.mock import Mock
from egon_validation.rules.registry import (
    register,
    register_map,
    rules_for,
    list_registered,
    _REGISTRY,
)
from egon_validation.rules.base import Rule, SqlRule, RuleResult, Severity


class MockRule(Rule):
    def evaluate(self, engine, ctx):
        return RuleResult(self.rule_id, self.task, self.table, True)


class MockSqlRule(SqlRule):
    def sql(self, ctx):
        return "SELECT 1"

    def postprocess(self, row, ctx):
        return RuleResult(self.rule_id, self.task, self.table, True)


class TestRegistry:
    def setup_method(self):
        # Clear registry before each test
        _REGISTRY.clear()

    def teardown_method(self):
        # Clear registry after each test
        _REGISTRY.clear()

    def test_register_decorator_basic(self):
        @register(task="test_task", table="test.table")
        class TestValidationRule(MockRule):
            pass

        assert len(_REGISTRY) == 1
        rule_id, task, dataset, rule_cls, params, kind = _REGISTRY[0]

        assert rule_id == "TestValidationRule"  # Default to class name
        assert task == "test_task"
        assert dataset == "test.table"
        assert rule_cls == TestValidationRule
        assert params == {}
        assert kind == "formal"  # Default

    def test_register_decorator_with_rule_id(self):
        @register(task="test_task", table="test.table", rule_id="CUSTOM_RULE_ID")
        class AnotherRule(MockRule):
            pass

        rule_id, _, _, _, _, _ = _REGISTRY[0]
        assert rule_id == "CUSTOM_RULE_ID"

    def test_register_decorator_with_params(self):
        @register(
            task="test_task",
            table="test.table",
            kind="custom",
            column="test_col",
            threshold=0.5,
            scenario="test_scenario",
        )
        class ParameterizedRule(MockRule):
            pass

        rule_id, task, dataset, rule_cls, params, kind = _REGISTRY[0]

        assert kind == "custom"
        assert params["scenario"] == "test_scenario"
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

    def test_register_map_single_dataset(self):
        register_map(
            task="test_task",
            rule_cls=MockSqlRule,
            rule_id="MAP_RULE",
            kind="custom",
            datasets_params={"schema1.table1": {"column": "col1", "threshold": 10}},
        )

        assert len(_REGISTRY) == 1
        rule_id, task, dataset, rule_cls, params, kind = _REGISTRY[0]

        assert rule_id == "MAP_RULE"
        assert task == "test_task"
        assert dataset == "schema1.table1"
        assert rule_cls == MockSqlRule
        assert params["column"] == "col1"
        assert params["threshold"] == 10
        assert kind == "custom"

    def test_register_map_multiple_datasets(self):
        register_map(
            task="test_task",
            rule_cls=MockSqlRule,
            datasets_params={
                "schema1.table1": {"column": "col1", "threshold": 10},
                "schema2.table2": {"column": "col2", "threshold": 20},
            },
        )

        assert len(_REGISTRY) == 2

        # Check first registration
        rule_id1, task1, dataset1, rule_cls1, params1, kind1 = _REGISTRY[0]
        assert dataset1 == "schema1.table1"
        assert params1["column"] == "col1"
        assert params1["threshold"] == 10

        # Check second registration
        rule_id2, task2, dataset2, rule_cls2, params2, kind2 = _REGISTRY[1]
        assert dataset2 == "schema2.table2"
        assert params2["column"] == "col2"
        assert params2["threshold"] == 20

    def test_register_map_default_rule_id(self):
        register_map(
            task="test_task", rule_cls=MockSqlRule, datasets_params={"test.table": {}}
        )

        rule_id, _, _, _, _, _ = _REGISTRY[0]
        assert rule_id == "MockSqlRule"  # Should default to class name

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

        # Get rules for task1
        rules_task1 = list(rules_for("task1"))
        assert len(rules_task1) == 2

        # Check that rules are properly instantiated
        rule_names = [rule.rule_id for rule in rules_task1]
        assert "Rule1" in rule_names
        assert "Rule3" in rule_names

        # Check parameters are passed correctly
        rule1 = next(rule for rule in rules_task1 if rule.rule_id == "Rule1")
        assert rule1.params["param1"] == "value1"
        assert "kind" in rule1.params  # kind should be added automatically

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
            table="table1",
            rule_id="RULE1",
            kind="custom",
            column="col1",
            scenario="test_scenario",
        )
        class Rule1(MockRule):
            pass

        @register(task="task2", table="table2")
        class Rule2(MockRule):
            pass

        registered = list_registered()
        assert len(registered) == 2

        # Check first rule
        rule1_info = next(r for r in registered if r["rule_id"] == "RULE1")
        assert rule1_info["task"] == "task1"
        assert rule1_info["dataset"] == "table1"
        assert rule1_info["kind"] == "custom"
        assert rule1_info["params"]["scenario"] == "test_scenario"
        assert rule1_info["params"]["column"] == "col1"

        # Check second rule
        rule2_info = next(r for r in registered if r["rule_id"] == "Rule2")
        assert rule2_info["task"] == "task2"
        assert rule2_info["dataset"] == "table2"
        assert rule2_info["kind"] == "formal"
        assert "scenario" not in rule2_info["params"]

    def test_register_preserves_original_class(self):
        @register(task="test_task", table="test.table")
        class OriginalRule(MockRule):
            custom_method = lambda self: "test"

        # The decorator should return the original class unchanged
        assert hasattr(OriginalRule, "custom_method")
        # Need to instantiate with required parameters
        instance = OriginalRule("test_rule", "test_task", "test.table")
        assert instance.custom_method() == "test"

    def test_register_map_preserves_params(self):
        original_params = {"column": "col1", "threshold": 10}

        register_map(
            task="test_task",
            rule_cls=MockSqlRule,
            datasets_params={"test.table": original_params},
        )

        # Original params dict should not be modified
        assert original_params == {"column": "col1", "threshold": 10}

        # Registry should have a copy
        _, _, _, _, stored_params, _ = _REGISTRY[0]
        assert stored_params == original_params
        assert stored_params is not original_params  # Should be a copy

    def test_rules_for_instantiation(self):
        @register(
            task="test_task", table="schema.table", kind="custom", param="value"
        )
        class TestRuleForInstantiation(MockRule):
            pass

        rules = list(rules_for("test_task"))
        rule = rules[0]

        # Check that rule is properly instantiated
        assert isinstance(rule, TestRuleForInstantiation)
        assert rule.rule_id == "TestRuleForInstantiation"
        assert rule.task == "test_task"
        assert rule.table == "schema.table"
        assert rule.schema == "schema"
        assert rule.table == "table"
        assert rule.params["param"] == "value"
        assert rule.params["kind"] == "custom"

    def test_registry_thread_safety_assumption(self):
        # This test documents the current behavior - the registry is a module-level list
        # In a multi-threaded environment, this could be problematic

        initial_count = len(_REGISTRY)

        @register(task="concurrent_task", table="test.table")
        class ConcurrentRule(MockRule):
            pass

        assert len(_REGISTRY) == initial_count + 1

        # The registry is shared globally
        assert _REGISTRY[-1][0] == "ConcurrentRule"
