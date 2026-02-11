# Changelog

All notable changes to this project will be documented in this file.

## [1.3.0] - 2026-02-11

### Changed
- Renamed `sql()` to `get_query()` across all SqlRule subclasses for consistency
- Moved `get_query()` to base `Rule` class as abstract method
- Moved `_check_table_empty()` from `SqlRule` to base `Rule` class (now available for DataFrameRule)
- DataFrameRule now checks for empty tables before loading DataFrame

### Fixed
- Parameter name mismatch in `GeometryContainmentValidation.postprocess()` (`reference_filter` → `ref_filter`)
- Test parameter names to match rule implementations (`reference_dataset` → `ref_table`, etc.)
- Test assertions in `test_coverage_analysis.py`

### Maintenance
- Removed unused imports across test files
- Applied black formatting
- Fixed linting issues (E731 lambda expression)

## [1.2.3] - 2026-01-15

### Maintenance
- Documentation updates
- Minor code cleanup

## [1.2.2] - 2025-12-20

### Changed
- Renamed rule classes to follow `*Validation` naming convention (e.g., `NotNullAndNotNaNValidation`)
- Added `_parse_table_name()` helper for table name parsing
- Moved `get_schema_and_table()` to base.py for reuse
- Added `create_result()` factory method in base rule class
- Added default values for database connection parameters
- Updated custom rule names in HTML matrix display
- Updated empty tables message in reports

### Added
- `WholeTableNotNullAndNotNaNValidation` rule for validating all columns automatically
- Dynamic version display in HTML reports
- Automatic rule initialization

### Fixed
- Directory handling for validation runs
- Report generation issues

## [1.1.1] - 2025-11-21

### Changed
- Upgraded to Python 3.10
- Renamed `dataset` parameter to `table` in rule definitions
- Refactored rule initialization with centralized code
- Updated dependencies for egon-data compatibility
- Improved Details section in HTML report

### Added
- Inline validation declaration support
- Execution time tracking for rules
- `__init__` methods to all validation classes
- Logging for table count during validation
- Report assets included in package distribution

### Fixed
- Test bugs and rule discovery issues
- GeoAlchemy compatibility
- Coverage test calculations
- Return statement handling in rules

## [1.0.0] - 2025-09-29

### Added
- Initial stable release
- Core validation framework with rule system
- SQL-based and Python-based rule support
- Built-in formal validation rules:
  - `NullCheck` - NULL value validation
  - `DataTypeCheck` - Column data type validation
  - `SRIDUniqueNonZero` - PostGIS SRID validation
  - `GeometryCheck` - PostGIS geometry validity
  - `ReferentialIntegrityCheck` - Foreign key validation
  - `RowCountCheck` - Row count validation
  - `ValueSetCheck` - Allowed values validation
  - `ArrayCardinalityCheck` - Array length validation
- Custom domain-specific rules (numeric aggregation checks)
- Interactive HTML reporting with filtering and sorting
- JSON export for machine-readable results
- Coverage analysis for rules and tables
- CLI interface (`run-task`, `final-report`)
- SSH tunnel support for secure remote database access
- Parallel rule execution with configurable worker threads
- Tolerance support for validation thresholds
- Airflow integration support
- Comprehensive test suite
- Logging configuration with retry logic

### Changed
- Improved docstrings across codebase
- Removed redundant code and scripts
- Restructured test directory
- Applied linter changes for code quality

### Fixed
- Database connection pooling and threading issues
- HTML report generation and table ordering
- Coverage analysis for missing tables
- Rule registration and discovery

---

## Development History

### 2025-09-27
- Merge pull request #3 from sagemaso/dev
- Add license text (AGPL-3.0)
- Delete redundant scripts
- Improve docstrings

### 2025-09-25
- Merge pull request #2 from sagemaso/dev
- Add logs and validation results

### 2025-09-24
- Test old geometry rules
- Change task naming, improve HTML output
- Structure tests and settings

### 2025-09-22
- Import logger configuration
- Delete evaluate method from base class
- Adjust dependencies
- Debug numeric aggregation check
- Include German border for geometry check

### 2025-09-15
- Improve test directory structure
- Add tests for formal rules

### 2025-09-12
- Add linter and apply changes
- Add retry logic
- Add logger configuration
- Set pandas version constraint
- Adjust worker configuration

### 2025-09-03
- Update pandas and geopandas dependencies

### 2025-08-30
- Add dataframe adapter

### 2025-08-28
- Add examples
- Add tests
- Edit README
- Add docstrings

### 2025-08-27
- Delete redundant code

### 2025-08-26
- Delete unused variables
- Remove sanity rules, add unit tests

### 2025-08-23
- Merge pull request #1 from sagemaso/dev
- Remove scenario from top-level call
- Clean code
- Debug rule coverage, sort rule application statistics
- Debug missing tables in HTML

### 2025-08-22
- Add clean README
- Add coverage analysis, edit report.html, remove scenario

### 2025-08-20
- Order tables alphabetically in report.html
- Improve structure of rules/custom/sanity
- Improve report.html: header, symbols, columns

### 2025-08-19
- Remove validation_runs from git tracking, add .gitignore
- Debug rules

### 2025-08-17
- Use connection pooling and threading
- Add new formal rules
- Debug sanity gas rules

### 2025-08-13
- Start debugging sanity rules

### 2025-08-12
- Adjust runner and registry
- Add sanity rules to adhoc run
- Add registry automation
- Add database connection
- Adjust reporting

### 2025-08-10
- Add sanity rules
- Initial project skeleton

[1.3.0]: https://github.com/yourusername/egon-validation/releases/tag/v1.3.0
[1.2.3]: https://github.com/yourusername/egon-validation/releases/tag/v1.2.3
[1.2.2]: https://github.com/yourusername/egon-validation/releases/tag/v1.2.2
[1.1.1]: https://github.com/yourusername/egon-validation/releases/tag/v1.1.1
[1.0.0]: https://github.com/yourusername/egon-validation/releases/tag/v1.0.0