# eGon Validation Framework

A comprehensive data validation library designed for the eGon data pipeline, providing robust data quality assurance with flexible rule definitions, detailed reporting, and coverage analysis.

## Features

### Core Capabilities
- **Database Integration**: Native PostgreSQL/PostGIS support via SQLAlchemy and psycopg2
- **Flexible Rule System**: Extensible formal and custom validation rules
- **Performance Optimized**: SQL-first execution with minimal memory footprint
- **Rich Reporting**: Interactive HTML reports with detailed results and coverage analysis
- **Production Ready**: Airflow-compatible CLI with unique run tracking

### Technical Highlights
- **Python 3.8** compatible
- **Zero-config** for basic usage with sensible defaults
- **Tolerance Support**: Configurable acceptance thresholds for validation rules
- **Resume-safe**: Unique run IDs enable interrupted workflow recovery
- **Thread-safe**: Parallel rule execution for improved performance
- **Open Source**: Licensed under AGPL-3.0

## Quick Start

### Installation

```bash
# Install in development mode
pip install -e .

# Install with test dependencies
pip install -e ".[test]"
```

### Basic Usage

#### Option 1: Automated Validation (Recommended)

```bash
# Configure database connection (see Configuration section)
# Then run the automated validation script
python3 dev/debug_with_trunnel.py
```

This script automatically:
- ✅ Generates a timestamped run ID (format: `validation-test-YYYYMMDDTHHMMSS`)
- ✅ Establishes secure database connection
- ✅ Executes all registered validation rules
- ✅ Generates comprehensive HTML and JSON reports
- ✅ Provides run summary and location of results

#### Option 2: Manual CLI Control

```bash
# Generate unique run ID
RUNID="validation-$(date +%Y%m%dT%H%M%S)"

# Run validation with SSH tunnel
egon-validation run-task --run-id $RUNID --task validation-test --with-tunnel

# Generate final reporter
egon-validation final-report --run-id $RUNID

# Alternative: Direct database connection
egon-validation run-task --db-url "postgresql://user:pass@host:port/db" --run-id $RUNID --task validation-test
```

### Viewing Results

Results are stored in `./validation_runs/<run_id>/`:
- 📊 **Interactive Report**: `final/report.html` - Open in browser for detailed analysis
- 📋 **Raw Results**: `final/results.json` - Machine-readable validation results
- 📈 **Coverage Analysis**: `final/coverage.json` - Rule and table coverage metrics
- 📝 **Detailed Logs**: `tasks/*/results.jsonl` - Per-task execution logs

## Structure

```
egon_validation/
  config.py          # .env/ENV helpers, defaults
  context.py         # RunContext(run_id, out_dir, extra)
  db.py              # DB helpers (SQLAlchemy/psycopg2)
  cli.py             # dev CLI (run-task, final-report)

  rules/
    base.py          # Rule, SqlRule, PyRule, RuleResult, Severity
    registry.py      # @register decorator, rule discovery
    formal.py        # NotNullAndNotNaN, Range, AbsDiffWithinTolerance, SRIDUniqueNonZero
    custom.py        # example placeholder for custom checks

  runner/
    execute.py       # run_for_task(ctx, task) -> writes JSONL per task
    aggregate.py     # collect(ctx) -> results.json + coverage.json

  report/
    assets/
      report.html    # HTML (static, few placeholders)
      report.css     # CSS (independent)
      report.js      # JS (renders tables + matrix from JSON)
    generate.py      # copies assets, writes results/coverage JSON and replaces placeholders
```

## Configuration

### Database Connection

The validation framework supports multiple database connection methods:

#### Method 1: Environment Variables

```bash
# Set in your environment or .env file
export DB_HOST="your-database-host"
export DB_PORT="5432"
export DB_NAME="your-database-name"
export DB_USER="your-username"
export DB_PASS="your-password"
```

#### Method 2: Direct Database URL

```bash
# PostgreSQL connection string
export DB_URL="postgresql://user:password@host:port/database"
```

#### Method 3: SSH Tunnel (Secure Remote Access)

```bash
# Configure SSH tunnel parameters
export SSH_HOST="your-ssh-gateway"
export SSH_USER="your-ssh-username"
# Database accessed through tunnel
export DB_HOST="localhost"  # accessed via tunnel
```

### Validation Configuration

Customize validation behavior through environment variables:

```bash
# Validation settings
export MAX_WORKERS="6"                    # Parallel execution threads
export OUTPUT_DIR="./validation_runs"     # Results storage location
export DEFAULT_TOLERANCE="0.0"           # Default acceptance tolerance
```

## Architecture Overview

### Core Components

#### 1. Rule System (`egon_validation/rules/`)
- **Base Classes**: Abstract rule definitions and result structures
- **Formal Rules**: Standard validation patterns (null checks, ranges, data types)
- **Custom Rules**: Domain-specific validation logic
- **Registry**: Automatic rule discovery and registration system

#### 2. Execution Engine (`egon_validation/runner/`)
- **Parallel Execution**: Multi-threaded rule processing
- **Coverage Analysis**: Comprehensive validation coverage tracking
- **Result Aggregation**: Consolidation of validation outcomes

#### 3. Reporting System (`egon_validation/report/`)
- **Interactive HTML**: Rich web-based reports with filtering and sorting
- **JSON Export**: Machine-readable results for integration
- **Coverage Matrix**: Visual representation of validation coverage

#### 4. Database Layer (`egon_validation/db.py`)
- **Connection Management**: SQLAlchemy-based database abstraction
- **Query Optimization**: Efficient SQL execution with proper connection pooling
- **SSH Tunnel Support**: Secure remote database access

### Data Flow

```mermaid
flowchart TD
    A[CLI Command] --> B[Load Configuration]
    B --> C[Initialize Database Connection]
    C --> D[Discover Registered Rules]
    D --> E[Execute Rules in Parallel]
    E --> F[Collect Results]
    F --> G[Generate Coverage Analysis]
    G --> H[Create Reports]
    H --> I[Store Results by Run ID]
```

## Rule Development Guide

### Creating Custom Validation Rules

#### 1. SQL-Based Rules (Recommended)

```python
from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

@register(
    task="data_quality",           # Task group this rule belongs to
    dataset="public.load_profiles", # Target table (schema.table)
    rule_id="LP_VALUE_RANGE",      # Unique identifier
    kind="formal",                 # Rule category: 'formal' or 'custom'
    column="value",                # Target column
    min_value=0.0,                 # Rule-specific parameters
    max_value=1000.0,
    tolerance=0.01                 # Acceptable deviation
)
class ValueRangeCheck(SqlRule):
    """Validates that values fall within acceptable range."""
    
    def sql(self, ctx):
        """Generate SQL query for validation.
        
        Args:
            ctx: RunContext containing run_id and other metadata
            
        Returns:
            str: SQL query that returns validation results
        """
        col = self.params.get("column")
        min_val = self.params.get("min_value")
        max_val = self.params.get("max_value")
        
        query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(CASE WHEN {col} < {min_val} OR {col} > {max_val} THEN 1 END) as invalid_rows,
            MIN({col}) as observed_min,
            MAX({col}) as observed_max
        FROM {self.dataset}
        """
            
        return query
    
    def postprocess(self, row, ctx):
        """Process SQL query results into validation outcome.
        
        Args:
            row: Dictionary containing SQL query results
            ctx: RunContext with execution metadata
            
        Returns:
            RuleResult: Validation outcome with success status and details
        """
        total_rows = int(row["total_rows"] or 0)
        invalid_rows = int(row["invalid_rows"] or 0)
        
        # Apply tolerance if specified
        tolerance = self.params.get("tolerance", 0.0)
        acceptable_failures = int(total_rows * tolerance)
        
        success = invalid_rows <= acceptable_failures
        
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            dataset=self.dataset,
            success=success,
            observed=invalid_rows,
            expected=f"≤{acceptable_failures}",
            message=f"{invalid_rows}/{total_rows} values outside range [{self.params['min_value']}, {self.params['max_value']}]",
            severity=Severity.ERROR if not success else Severity.WARNING,
            schema=self.schema,
            table=self.table,
            column=self.params.get("column")
        )
```

#### 2. Python-Based Rules (Advanced)

```python
from egon_validation.rules.base import Rule, RuleResult, Severity
from egon_validation.rules.registry import register
from egon_validation import db

@register(
    task="data_integrity",
    dataset="public.network_nodes",
    rule_id="GEOM_TOPOLOGY_CHECK",
    kind="custom"
)
class GeometryTopologyRule(Rule):
    """Advanced geometry validation using PostGIS functions."""
    
    def evaluate(self, engine, ctx):
        """Custom evaluation logic for complex validation scenarios.
        
        Args:
            engine: SQLAlchemy database engine
            ctx: RunContext with execution metadata
            
        Returns:
            RuleResult: Validation outcome
        """
        try:
            # Custom logic using multiple queries
            query1 = f"SELECT COUNT(*) as total FROM {self.dataset}"
            result1 = db.fetch_one(engine, query1)
            
            query2 = f"""
            SELECT COUNT(*) as invalid 
            FROM {self.dataset} 
            WHERE NOT ST_IsValid(geom)
            """
            result2 = db.fetch_one(engine, query2)
            
            total = int(result1["total"])
            invalid = int(result2["invalid"])
            
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                dataset=self.dataset,
                success=(invalid == 0),
                observed=invalid,
                expected=0,
                message=f"{invalid}/{total} geometries have topology errors",
                severity=Severity.ERROR,
                schema=self.schema,
                table=self.table
            )
            
        except Exception as e:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                dataset=self.dataset,
                success=False,
                message=f"Rule execution failed: {str(e)}",
                severity=Severity.ERROR
            )
```

### Rule Registration Patterns

#### Bulk Rule Registration

```python
from egon_validation.rules.registry import register_map
from egon_validation.rules.formal.null_check import NotNullCheck

# Register the same rule for multiple datasets
register_map(
    task="data_completeness",
    rule_cls=NotNullCheck,
    rule_id="NULL_CHECK",
    kind="formal",
    datasets_params={
        "public.load_profiles": {"column": "value"},
        "public.network_buses": {"column": "v_nom"},
        "public.generators": {"column": "p_nom"},
    }
)
```

### Available Formal Rules

The framework includes built-in formal validation rules:

| Rule Class | Purpose | Parameters |
|------------|---------|------------|
| `NullCheck` | Validates no NULL values | `column` |
| `RangeCheck` | Validates numeric ranges | `column`, `min_value`, `max_value` |
| `DataTypeCheck` | Validates column data types | `column`, `expected_type` |
| `ValueSetCheck` | Validates against allowed values | `column`, `valid_values` |
| `SridCheck` | Validates geometry SRID | `column`, `expected_srid` |
| `RowCountCheck` | Validates expected row counts | `expected_count`, `tolerance` |
| `ReferentialIntegrityCheck` | Validates foreign key relationships | `column`, `reference_table`, `reference_column` |
| `GeometryCheck` | Validates PostGIS geometry validity | `column` |
| `BalanceCheck` | Validates sum/balance constraints | `column`, `expected_sum` |
| `ArrayCardinalityCheck` | Validates array element counts | `column`, `expected_length` |

## Testing and Quality Assurance

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage reporter
pytest --cov=egon_validation --cov-report=html

# Run specific test categories
pytest tests/test_formal_rules.py    # Test formal rules
pytest tests/test_custom_rules.py    # Test custom rules
pytest tests/test_registry.py        # Test rule registration
```

### Test Structure

The test suite covers:
- ✅ **Unit Tests**: Individual rule validation logic
- ✅ **Integration Tests**: End-to-end validation workflows
- ✅ **Coverage Tests**: Rule discovery and registrationCheck

## Integration with eGon-data Pipeline

### Airflow Integration

The CLI interface is designed for seamless Airflow integration:

```python
# Airflow DAG example
from airflow.operators.bash import BashOperator
from datetime import datetime

validation_task = BashOperator(
    task_id='data_validation',
    bash_command='''
        RUNID="{{ ds }}_{{ ts_nodash }}"
        egon-validation run-task --run-id $RUNID --task {{ params.task_name }}
        egon-validation final-reporter --run-id $RUNID
    ''',
    params={'task_name': 'data_quality_check'}
)
```

### Exit Codes and Error Handling

- **Exit Code 0**: All validations completed successfully
- **Exit Code 1**: Validation execution errors (database connectivity, rule failures)
- **Exit Code 2**: Configuration or setup errors

The framework is designed to be non-blocking - validation failures generate reports but don't halt pipeline execution.