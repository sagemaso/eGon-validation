import pytest
from unittest.mock import Mock, patch
from egon_validation.rules.custom.numeric_aggregation_check import DisaggregatedDemandSumValidation
from egon_validation.rules.base import RuleResult, Severity


class TestDisaggregatedDemandSumValidation:
    def test_sql_generation_default_sector(self):
        rule = DisaggregatedDemandSumValidation(
            rule_id="test_rule",
            table="demand.egon_demandregio_zensus_electricity",
            task="test_task",
        )
        sql = rule.get_query(None)

        assert "WITH disaggregated AS" in sql
        assert "sector = 'residential'" in sql
        assert "demand.egon_demandregio_hh" in sql
        assert "ABS(d.disagg_sum - o.orig_sum)" in sql

    def test_sql_generation_custom_sector(self):
        rule = DisaggregatedDemandSumValidation(
            rule_id="test_rule",
            table="demand.egon_demandregio_zensus_electricity",
            task="test_task",
            sector="commercial",
        )
        sql = rule.get_query(None)

        assert "sector = 'commercial'" in sql

    def test_postprocess_within_tolerance(self):
        rule = DisaggregatedDemandSumValidation(
            rule_id="test_rule",
            table="demand.egon_demandregio_zensus_electricity",
            task="test_task",
            tolerance=0.05,
        )

        row = {
            "scenario": "eGon2035",
            "disagg_sum": 1000.0,
            "orig_sum": 1020.0,  # 2% difference
            "abs_diff": 20.0,
            "rel_diff": 0.0196,  # Within 5% tolerance
        }

        result = rule.postprocess(row, None)

        assert result.success is True
        assert "Scenario eGon2035" in result.message
        assert "Rel. diff 0.0196" in result.message
        assert result.observed == 0.0196
        assert result.expected == 0.05  # expected is the tolerance

    def test_postprocess_outside_tolerance(self):
        rule = DisaggregatedDemandSumValidation(
            rule_id="test_rule",
            table="demand.egon_demandregio_zensus_electricity",
            task="test_task",
            tolerance=0.01,
        )

        row = {
            "scenario": "eGon2035",
            "disagg_sum": 1000.0,
            "orig_sum": 1050.0,  # 5% difference
            "abs_diff": 50.0,
            "rel_diff": 0.0476,  # Outside 1% tolerance
        }

        result = rule.postprocess(row, None)

        assert result.success is False
        assert result.observed == 0.0476

    def test_postprocess_default_tolerance(self):
        # Use actual config default tolerance: DISAGGREGATED_DEMAND_TOLERANCE = 0.01
        rule = DisaggregatedDemandSumValidation(
            rule_id="test_rule",
            table="demand.egon_demandregio_zensus_electricity",
            task="test_task",
        )

        row = {
            "scenario": "test",
            "disagg_sum": 1000.0,
            "orig_sum": 1005.0,  # 0.5% difference
            "abs_diff": 5.0,
            "rel_diff": 0.005,  # Within 1% default tolerance
        }

        result = rule.postprocess(row, None)

        assert result.success is True
        assert "tolerance 0.01" in result.message

    def test_postprocess_none_values(self):
        rule = DisaggregatedDemandSumValidation(
            rule_id="test_rule",
            table="demand.egon_demandregio_zensus_electricity",
            task="test_task",
        )

        row = {
            "scenario": None,
            "disagg_sum": None,
            "orig_sum": None,
            "abs_diff": None,
            "rel_diff": None,
        }

        result = rule.postprocess(row, None)

        # Should handle None values gracefully
        assert "Scenario None" in result.message  # scenario info is in the message
        assert result.observed == 0.0

    def test_postprocess_zero_original_sum(self):
        rule = DisaggregatedDemandSumValidation(
            rule_id="test_rule",
            table="demand.egon_demandregio_zensus_electricity",
            task="test_task",
        )

        row = {
            "scenario": "test",
            "disagg_sum": 0.0,
            "orig_sum": 0.0,
            "abs_diff": 0.0,
            "rel_diff": 0.0,  # Would be NaN if not handled by NULLIF
        }

        result = rule.postprocess(row, None)

        assert result.success is True
        assert result.observed == 0.0