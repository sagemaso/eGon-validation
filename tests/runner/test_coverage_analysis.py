import pytest
import os
import json
import tempfile
from unittest.mock import Mock, patch, mock_open
from egon_validation.runner.coverage_analysis import (
    discover_total_tables,
    load_saved_table_count,
    calculate_coverage_stats,
)
from egon_validation.context import RunContext


class TestDiscoverTotalTables:
    @patch("egon_validation.runner.coverage_analysis.make_engine")
    @patch("egon_validation.runner.coverage_analysis.fetch_one")
    @patch("egon_validation.runner.coverage_analysis.get_env")
    @patch("egon_validation.runner.coverage_analysis.build_db_url")
    def test_discover_total_tables_success(self, mock_build_db_url, mock_get_env, mock_fetch_one, mock_make_engine):
        """Test successful discovery of total tables"""
        mock_get_env.return_value = "postgresql://user:pass@localhost:5432/db"
        mock_build_db_url.return_value = None
        mock_engine = Mock()
        mock_make_engine.return_value = mock_engine
        mock_fetch_one.return_value = {"total_tables": 42}

        result = discover_total_tables()

        assert result == 42
        mock_make_engine.assert_called_once_with("postgresql://user:pass@localhost:5432/db")
        mock_fetch_one.assert_called_once()
        # Verify the SQL query structure
        call_args = mock_fetch_one.call_args
        query = call_args[0][1]
        assert "COUNT(*)" in query
        assert "pg_tables" in query
        assert "schemaname NOT IN" in query
        assert "information_schema" in query
        assert "pg_catalog" in query

    @patch("egon_validation.runner.coverage_analysis.make_engine")
    @patch("egon_validation.runner.coverage_analysis.fetch_one")
    @patch("egon_validation.runner.coverage_analysis.get_env")
    @patch("egon_validation.runner.coverage_analysis.build_db_url")
    def test_discover_total_tables_fallback_to_build_db_url(self, mock_build_db_url, mock_get_env, mock_fetch_one, mock_make_engine):
        """Test fallback to build_db_url when ENV_DB_URL is not set"""
        mock_get_env.return_value = None
        mock_build_db_url.return_value = "postgresql://fallback:pass@localhost:5432/db"
        mock_engine = Mock()
        mock_make_engine.return_value = mock_engine
        mock_fetch_one.return_value = {"total_tables": 15}

        result = discover_total_tables()

        assert result == 15
        mock_build_db_url.assert_called_once()
        mock_make_engine.assert_called_once_with("postgresql://fallback:pass@localhost:5432/db")

    @patch("egon_validation.runner.coverage_analysis.get_env")
    @patch("egon_validation.runner.coverage_analysis.build_db_url")
    def test_discover_total_tables_no_db_url(self, mock_build_db_url, mock_get_env):
        """Test when no database URL is available"""
        mock_get_env.return_value = None
        mock_build_db_url.return_value = None

        result = discover_total_tables()

        assert result == 0

    @patch("egon_validation.runner.coverage_analysis.make_engine")
    @patch("egon_validation.runner.coverage_analysis.get_env")
    def test_discover_total_tables_database_error(self, mock_get_env, mock_make_engine):
        """Test handling of database connection errors"""
        mock_get_env.return_value = "postgresql://user:pass@localhost:5432/db"
        mock_make_engine.side_effect = Exception("Connection failed")

        result = discover_total_tables()

        assert result == 0

    @patch("egon_validation.runner.coverage_analysis.make_engine")
    @patch("egon_validation.runner.coverage_analysis.fetch_one")
    @patch("egon_validation.runner.coverage_analysis.get_env")
    def test_discover_total_tables_fetch_error(self, mock_get_env, mock_fetch_one, mock_make_engine):
        """Test handling of fetch errors"""
        mock_get_env.return_value = "postgresql://user:pass@localhost:5432/db"
        mock_engine = Mock()
        mock_make_engine.return_value = mock_engine
        mock_fetch_one.side_effect = Exception("Query failed")

        result = discover_total_tables()

        assert result == 0

    @patch("egon_validation.runner.coverage_analysis.make_engine")
    @patch("egon_validation.runner.coverage_analysis.fetch_one")
    @patch("egon_validation.runner.coverage_analysis.get_env")
    def test_discover_total_tables_missing_key(self, mock_get_env, mock_fetch_one, mock_make_engine):
        """Test handling when result doesn't contain expected key"""
        mock_get_env.return_value = "postgresql://user:pass@localhost:5432/db"
        mock_engine = Mock()
        mock_make_engine.return_value = mock_engine
        mock_fetch_one.return_value = {}  # Missing 'total_tables' key

        result = discover_total_tables()

        assert result == 0


class TestLoadSavedTableCount:
    def test_load_saved_table_count_success(self):
        """Test successful loading of saved table count"""
        mock_ctx = Mock()
        mock_ctx.out_dir = "/test/output"
        mock_ctx.run_id = "test_run_123"

        metadata = {"total_tables": 25}
        with patch("builtins.open", mock_open(read_data=json.dumps(metadata))):
            with patch("os.path.exists", return_value=True):
                result = load_saved_table_count(mock_ctx)

        assert result == 25

    def test_load_saved_table_count_file_not_exists(self):
        """Test when metadata file doesn't exist"""
        mock_ctx = Mock()
        mock_ctx.out_dir = "/test/output"
        mock_ctx.run_id = "test_run_123"

        with patch("os.path.exists", return_value=False):
            result = load_saved_table_count(mock_ctx)

        assert result == 0

    def test_load_saved_table_count_json_error(self):
        """Test handling of JSON parsing errors"""
        mock_ctx = Mock()
        mock_ctx.out_dir = "/test/output"
        mock_ctx.run_id = "test_run_123"

        with patch("builtins.open", mock_open(read_data="invalid json")):
            with patch("os.path.exists", return_value=True):
                result = load_saved_table_count(mock_ctx)

        assert result == 0

    def test_load_saved_table_count_missing_key(self):
        """Test when metadata file exists but doesn't contain total_tables key"""
        mock_ctx = Mock()
        mock_ctx.out_dir = "/test/output"
        mock_ctx.run_id = "test_run_123"

        metadata = {"other_key": "value"}
        with patch("builtins.open", mock_open(read_data=json.dumps(metadata))):
            with patch("os.path.exists", return_value=True):
                result = load_saved_table_count(mock_ctx)

        assert result == 0

    def test_load_saved_table_count_file_error(self):
        """Test handling of file read errors"""
        mock_ctx = Mock()
        mock_ctx.out_dir = "/test/output"
        mock_ctx.run_id = "test_run_123"

        with patch("os.path.exists", return_value=True):
            with patch("builtins.open", side_effect=IOError("File read error")):
                result = load_saved_table_count(mock_ctx)

        assert result == 0

    def test_load_saved_table_count_correct_path(self):
        """Test that the correct metadata file path is constructed"""
        mock_ctx = Mock()
        mock_ctx.out_dir = "/custom/output"
        mock_ctx.run_id = "custom_run_456"

        expected_path = "/custom/output/custom_run_456/tasks/db_metadata.json"

        with patch("os.path.exists") as mock_exists:
            mock_exists.return_value = False
            load_saved_table_count(mock_ctx)
            mock_exists.assert_called_once_with(expected_path)


class TestCalculateCoverageStats:
    @patch("egon_validation.runner.coverage_analysis.discover_all_rule_classes")
    @patch("egon_validation.runner.coverage_analysis.load_saved_table_count")
    def test_calculate_coverage_stats_basic(self, mock_load_saved, mock_discover_rules):
        """Test basic coverage statistics calculation"""
        mock_discover_rules.return_value = {"RuleClass1", "RuleClass2", "RuleClass3"}
        mock_load_saved.return_value = 50

        collected_data = {
            "items": [
                {"rule_class": "RuleClass1", "success": True},
                {"rule_class": "RuleClass1", "success": False},
                {"rule_class": "RuleClass2", "success": True},
            ],
            "datasets": ["schema1.table1", "schema2.table2"]
        }

        mock_ctx = Mock()
        result = calculate_coverage_stats(collected_data, mock_ctx)

        expected = {
            "table_coverage": {
                "validated_tables": 2,
                "total_tables": 50,
                "percentage": 4.0,
            },
            "rule_coverage": {
                "applied_rules": 2,  # RuleClass1 and RuleClass2 applied
                "total_rules": 3,
                "percentage": 66.7,
            },
            "validation_results": {
                "total_applications": 3,
                "successful": 2,
                "failed": 1,
                "success_rate": 66.7,
            },
            "rule_application_stats": [
                {"rule_class": "RuleClass1", "applications": 2},
                {"rule_class": "RuleClass2", "applications": 1},
            ],
        }

        assert result == expected

    @patch("egon_validation.runner.coverage_analysis.discover_all_rule_classes")
    @patch("egon_validation.runner.coverage_analysis.discover_total_tables")
    def test_calculate_coverage_stats_no_ctx_fallback_to_discover(self, mock_discover, mock_discover_rules):
        """Test fallback to discover_total_tables when no context provided"""
        mock_discover_rules.return_value = set()
        mock_discover.return_value = 25

        collected_data = {"items": [], "datasets": []}

        result = calculate_coverage_stats(collected_data, ctx=None)

        assert result["table_coverage"]["total_tables"] == 25
        mock_discover.assert_called_once()

    @patch("egon_validation.runner.coverage_analysis.discover_all_rule_classes")
    @patch("egon_validation.runner.coverage_analysis.load_saved_table_count")
    @patch("egon_validation.runner.coverage_analysis.discover_total_tables")
    def test_calculate_coverage_stats_saved_count_fallback(self, mock_discover, mock_load_saved, mock_discover_rules):
        """Test fallback to discover when saved count is 0"""
        mock_discover_rules.return_value = set()
        mock_load_saved.return_value = 0
        mock_discover.return_value = 30

        collected_data = {"items": [], "datasets": []}
        mock_ctx = Mock()

        result = calculate_coverage_stats(collected_data, mock_ctx)

        assert result["table_coverage"]["total_tables"] == 30
        mock_load_saved.assert_called_once_with(mock_ctx)
        mock_discover.assert_called_once()

    @patch("egon_validation.runner.coverage_analysis.discover_all_rule_classes")
    def test_calculate_coverage_stats_empty_data(self, mock_discover_rules):
        """Test handling of empty collected data"""
        mock_discover_rules.return_value = set()

        collected_data = {"items": [], "datasets": []}

        result = calculate_coverage_stats(collected_data)

        expected = {
            "table_coverage": {
                "validated_tables": 0,
                "total_tables": 0,
                "percentage": 0,
            },
            "rule_coverage": {
                "applied_rules": 0,
                "total_rules": 0,
                "percentage": 0,
            },
            "validation_results": {
                "total_applications": 0,
                "successful": 0,
                "failed": 0,
                "success_rate": 0,
            },
            "rule_application_stats": [],
        }

        assert result == expected

    @patch("egon_validation.runner.coverage_analysis.discover_all_rule_classes")
    def test_calculate_coverage_stats_missing_keys(self, mock_discover_rules):
        """Test handling of items without required keys"""
        mock_discover_rules.return_value = {"RuleClass1"}

        collected_data = {
            "items": [
                {},  # Missing rule_class and success - not counted
                {"rule_class": "RuleClass1"},  # Missing success - counted as failed
                {"success": True},  # Missing rule_class - not counted
                {"rule_class": "RuleClass1", "success": True},  # Valid item - counted as success
            ],
            "datasets": ["table1"]
        }

        result = calculate_coverage_stats(collected_data)

        # Implementation counts all items with rule_class, treating missing success as False
        assert result["rule_coverage"]["applied_rules"] == 1  # Only RuleClass1 counted
        assert result["validation_results"]["total_applications"] == 2  # Both items with rule_class
        assert result["validation_results"]["successful"] == 1  # Only item with success=True
        assert result["validation_results"]["failed"] == 1  # Item with missing success counts as failed
        assert result["rule_application_stats"] == [{"rule_class": "RuleClass1", "applications": 2}]

    @patch("egon_validation.runner.coverage_analysis.discover_all_rule_classes")
    def test_calculate_coverage_stats_multiple_rule_applications(self, mock_discover_rules):
        """Test multiple applications of the same rule"""
        mock_discover_rules.return_value = {"RuleClass1", "RuleClass2"}

        collected_data = {
            "items": [
                {"rule_class": "RuleClass1", "success": True},
                {"rule_class": "RuleClass1", "success": True},
                {"rule_class": "RuleClass1", "success": False},
                {"rule_class": "RuleClass2", "success": True},
            ],
            "datasets": ["table1", "table2", "table3"]
        }

        result = calculate_coverage_stats(collected_data)

        assert result["rule_coverage"]["applied_rules"] == 2
        assert result["validation_results"]["total_applications"] == 4
        assert result["validation_results"]["successful"] == 3
        assert result["validation_results"]["failed"] == 1
        assert result["validation_results"]["success_rate"] == 75.0

        # Check rule application stats
        rule_stats = {stat["rule_class"]: stat["applications"] for stat in result["rule_application_stats"]}
        assert rule_stats["RuleClass1"] == 3
        assert rule_stats["RuleClass2"] == 1

    @patch("egon_validation.runner.coverage_analysis.discover_all_rule_classes")
    def test_calculate_coverage_stats_percentage_calculations(self, mock_discover_rules):
        """Test percentage calculations with various scenarios"""
        mock_discover_rules.return_value = {"RuleClass1", "RuleClass2", "RuleClass3"}

        collected_data = {
            "items": [
                {"rule_class": "RuleClass1", "success": True},
                {"rule_class": "RuleClass2", "success": False},
            ],
            "datasets": ["table1", "table2"]
        }

        mock_ctx = Mock()
        with patch("egon_validation.runner.coverage_analysis.load_saved_table_count", return_value=10):
            result = calculate_coverage_stats(collected_data, mock_ctx)

        # Table coverage: 2/10 = 20%
        assert result["table_coverage"]["percentage"] == 20.0
        # Rule coverage: 2/3 = 66.7%
        assert result["rule_coverage"]["percentage"] == 66.7
        # Success rate: 1/2 = 50%
        assert result["validation_results"]["success_rate"] == 50.0

    def test_calculate_coverage_stats_missing_datasets_key(self):
        """Test handling when datasets key is missing from collected_data"""
        with patch("egon_validation.runner.coverage_analysis.discover_all_rule_classes", return_value=set()):
            collected_data = {"items": []}  # Missing 'datasets' key

            result = calculate_coverage_stats(collected_data)

            assert result["table_coverage"]["validated_tables"] == 0

    def test_calculate_coverage_stats_missing_items_key(self):
        """Test handling when items key is missing from collected_data"""
        with patch("egon_validation.runner.coverage_analysis.discover_all_rule_classes", return_value=set()):
            collected_data = {"datasets": []}  # Missing 'items' key

            result = calculate_coverage_stats(collected_data)

            assert result["validation_results"]["total_applications"] == 0
            assert result["rule_coverage"]["applied_rules"] == 0


class TestCoverageAnalysisIntegration:
    """Integration tests for coverage analysis functions working together"""

    def test_full_coverage_workflow_with_real_context(self):
        """Test complete coverage analysis workflow with realistic data"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a real RunContext
            ctx = RunContext(run_id="integration_test", out_dir=temp_dir)

            # Create metadata directory and file
            metadata_dir = os.path.join(temp_dir, "integration_test", "tasks")
            os.makedirs(metadata_dir, exist_ok=True)
            metadata_file = os.path.join(metadata_dir, "db_metadata.json")

            with open(metadata_file, "w") as f:
                json.dump({"total_tables": 100}, f)

            # Mock discovered rule classes
            mock_rule_classes = {"NullCheckValidation", "DataTypeValidation", "ValueSetValidation", "CustomRuleValidation"}

            # Realistic collected data with rule_class
            collected_data = {
                "items": [
                    {"rule_class": "NullCheckValidation", "success": True},
                    {"rule_class": "NullCheckValidation", "success": True},
                    {"rule_class": "DataTypeValidation", "success": False},
                    {"rule_class": "ValueSetValidation", "success": True},
                    {"rule_class": "CustomRuleValidation", "success": False},
                    {"rule_class": "CustomRuleValidation", "success": True},
                ],
                "datasets": [
                    "schema1.table1", "schema1.table2", "schema2.table3",
                    "grid.egon_data", "demand.load_data"
                ]
            }

            with patch("egon_validation.runner.coverage_analysis.discover_all_rule_classes", return_value=mock_rule_classes):
                result = calculate_coverage_stats(collected_data, ctx)

            # Verify realistic results
            assert result["table_coverage"]["validated_tables"] == 5
            assert result["table_coverage"]["total_tables"] == 100
            assert result["table_coverage"]["percentage"] == 5.0

            assert result["rule_coverage"]["applied_rules"] == 4  # All 4 rule classes used
            assert result["rule_coverage"]["total_rules"] == 4
            assert result["rule_coverage"]["percentage"] == 100.0

            assert result["validation_results"]["total_applications"] == 6
            assert result["validation_results"]["successful"] == 4
            assert result["validation_results"]["failed"] == 2
            assert result["validation_results"]["success_rate"] == 66.7

    @patch("egon_validation.runner.coverage_analysis.make_engine")
    @patch("egon_validation.runner.coverage_analysis.fetch_one")
    def test_discover_total_tables_with_various_table_counts(self, mock_fetch_one, mock_make_engine):
        """Test discover_total_tables with various realistic table counts"""
        mock_engine = Mock()
        mock_make_engine.return_value = mock_engine

        with patch("egon_validation.runner.coverage_analysis.get_env", return_value="postgresql://test"):
            # Test with zero tables
            mock_fetch_one.return_value = {"total_tables": 0}
            assert discover_total_tables() == 0

            # Test with small number of tables
            mock_fetch_one.return_value = {"total_tables": 5}
            assert discover_total_tables() == 5

            # Test with large number of tables
            mock_fetch_one.return_value = {"total_tables": 1000}
            assert discover_total_tables() == 1000

    def test_coverage_stats_edge_cases(self):
        """Test coverage statistics with various edge cases"""
        with patch("egon_validation.runner.coverage_analysis.discover_all_rule_classes") as mock_discover:
            # Case 1: More applied rule classes than discovered (external rules from pipeline)
            mock_discover.return_value = {"RuleClass1"}
            collected_data = {
                "items": [
                    {"rule_class": "RuleClass1", "success": True},
                    {"rule_class": "RuleClass2", "success": True},  # External rule not in codebase
                ],
                "datasets": ["table1"]
            }

            result = calculate_coverage_stats(collected_data)
            # Should count both applied rule classes
            # total_rules = union of discovered + applied = {"RuleClass1", "RuleClass2"} = 2
            assert result["rule_coverage"]["applied_rules"] == 2
            assert result["rule_coverage"]["total_rules"] == 2  # Union of discovered + applied
            assert result["rule_coverage"]["percentage"] == 100.0  # 2/2 = 100%

    def test_rounding_precision(self):
        """Test that percentage calculations are properly rounded"""
        with patch("egon_validation.runner.coverage_analysis.discover_all_rule_classes", return_value={"RuleClass1"}):
            with patch("egon_validation.runner.coverage_analysis.discover_total_tables", return_value=3):
                collected_data = {
                    "items": [{"rule_class": "RuleClass1", "success": True}],  # 1 success out of 1
                    "datasets": ["t1"]  # 1 table out of 3
                }

                result = calculate_coverage_stats(collected_data)

                # 1/3 = 33.333... should round to 33.3
                assert result["table_coverage"]["percentage"] == 33.3
                assert result["rule_coverage"]["percentage"] == 100.0
                assert result["validation_results"]["success_rate"] == 100.0


class TestCoverageAnalysisEdgeCases:
    """Test edge cases and error conditions"""

    def test_calculate_coverage_stats_with_none_values(self):
        """Test handling of None values in various places"""
        with patch("egon_validation.runner.coverage_analysis.list_registered", return_value=[]):
            # The function expects a dict, so None will raise AttributeError
            # This test documents the current behavior - the function doesn't handle None gracefully
            with pytest.raises(AttributeError):
                calculate_coverage_stats(None)

    def test_load_saved_table_count_with_invalid_json_structure(self):
        """Test loading with valid JSON but wrong structure"""
        mock_ctx = Mock()
        mock_ctx.out_dir = "/test"
        mock_ctx.run_id = "test"

        # Valid JSON but not the expected structure - these all should return 0 via exception handling
        invalid_structures = [
            "[]",  # Array instead of object
            "null",  # Null value
            '"string"',  # String instead of object
        ]

        for invalid_json in invalid_structures:
            with patch("builtins.open", mock_open(read_data=invalid_json)):
                with patch("os.path.exists", return_value=True):
                    result = load_saved_table_count(mock_ctx)
                    assert result == 0

        # Test string number case separately - with improved error handling this now returns 0
        # because the TypeError when comparing string to 0 is caught
        with patch("builtins.open", mock_open(read_data='{"total_tables": "not_a_number"}')):
            with patch("os.path.exists", return_value=True):
                result = load_saved_table_count(mock_ctx)
                # With improved error handling, this returns 0 instead of propagating the type error
                assert result == 0

    @patch("egon_validation.runner.coverage_analysis.make_engine")
    def test_discover_total_tables_engine_disposal(self, mock_make_engine):
        """Test that engine resources are properly handled even on errors"""
        mock_engine = Mock()
        mock_make_engine.return_value = mock_engine

        with patch("egon_validation.runner.coverage_analysis.get_env", return_value="postgresql://test"):
            with patch("egon_validation.runner.coverage_analysis.fetch_one", side_effect=Exception("DB Error")):
                result = discover_total_tables()
                assert result == 0
                # Engine should still be created even if fetch fails
                mock_make_engine.assert_called_once()

    def test_calculate_coverage_stats_very_large_numbers(self):
        """Test with very large numbers to ensure no overflow issues"""
        with patch("egon_validation.runner.coverage_analysis.discover_all_rule_classes", return_value=set()):
            with patch("egon_validation.runner.coverage_analysis.discover_total_tables", return_value=999999):
                collected_data = {
                    "items": [{"rule_class": f"RuleClass_{i}", "success": True} for i in range(100000)],
                    "datasets": [f"table_{i}" for i in range(50000)]
                }

                result = calculate_coverage_stats(collected_data)

                assert result["table_coverage"]["validated_tables"] == 50000
                assert result["table_coverage"]["total_tables"] == 999999
                assert result["rule_coverage"]["applied_rules"] == 100000
                assert result["validation_results"]["total_applications"] == 100000

    def test_rule_application_stats_sorting(self):
        """Test that rule application stats are properly sorted"""
        with patch("egon_validation.runner.coverage_analysis.discover_all_rule_classes", return_value=set()):
            collected_data = {
                "items": [
                    {"rule_class": "ZRuleClass", "success": True},
                    {"rule_class": "ARuleClass", "success": True},
                    {"rule_class": "MRuleClass", "success": True},
                    {"rule_class": "ARuleClass", "success": False},
                ],
                "datasets": []
            }

            result = calculate_coverage_stats(collected_data)

            # Should be sorted alphabetically by rule_class
            rule_classes = [stat["rule_class"] for stat in result["rule_application_stats"]]
            assert rule_classes == ["ARuleClass", "MRuleClass", "ZRuleClass"]

            # Check application counts
            rule_counts = {stat["rule_class"]: stat["applications"] for stat in result["rule_application_stats"]}
            assert rule_counts["ARuleClass"] == 2
            assert rule_counts["MRuleClass"] == 1
            assert rule_counts["ZRuleClass"] == 1