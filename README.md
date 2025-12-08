# eGon Validation Framework

A comprehensive data validation framework for the eGon data pipeline, providing robust quality assurance through flexible rule definitions, detailed reporting, and extensive coverage analysis.

## Features

- **Database Integration**: Native PostgreSQL/PostGIS support via SQLAlchemy
- **Flexible Rule System**: Extensible validation rules (formal and custom)
- **Performance Optimized**: SQL-first execution with minimal memory footprint
- **Rich Reporting**: Interactive HTML reports with detailed results and coverage matrices
- **Production Ready**: Airflow-compatible CLI with unique run tracking
- **Parallel Execution**: Thread-safe multi-rule processing
- **Tolerance Support**: Configurable acceptance thresholds for validation rules
- **Resume-safe**: Unique run IDs enable workflow recovery after interruptions
- **Open Source**: Licensed under AGPL-3.0

## Installation

```bash
# Install in development mode
pip install -e .

# Install with test dependencies
pip install -e ".[test]"
```

## Quick Start

### 1. Configure Database Connection

Choose one of the following methods:

```bash
# Option A: Environment variables
export DB_HOST="your-database-host"
export DB_PORT="5432"
export DB_NAME="egon-data"
export DB_USER="your-username"
export DB_PASS="your-password"

# Option B: Database URL
export DB_URL="postgresql://user:password@host:port/database"

# Option C: SSH tunnel (recommended for remote databases)
export SSH_HOST="your-ssh-gateway"
export SSH_USER="your-ssh-username"
export DB_HOST="localhost"
```

### 2. Run Validation

**Automated Script (Recommended):**

```bash
python3 dev/debug_with_trunnel.py
```

**Manual CLI:**

```bash
# Generate unique run ID
RUNID="validation-$(date +%Y%m%dT%H%M%S)"

# Run validation with SSH tunnel
egon-validation run-task --run-id $RUNID --task validation-test --with-tunnel

# Generate final report
egon-validation final-report --run-id $RUNID
```

### 3. View Results

Results are stored in `./validation_runs/<run_id>/`:
- **`final/report.html`** - Interactive HTML report with filtering and sorting
- **`final/results.json`** - Machine-readable validation results
- **`final/coverage.json`** - Rule and table coverage metrics
- **`tasks/*/results.jsonl`** - Per-task execution logs

## Structure

```
egon_validation/
  config.py          # Configuration and environment variables
  context.py         # RunContext for tracking validation runs
  db.py              # Database connection helpers
  cli.py             # CLI interface (run-task, final-report)

  rules/
    base.py          # Base classes: Rule, SqlRule, RuleResult, Severity
    registry.py      # @register decorator, rule discovery
    formal/          # Built-in validation rules
    custom/          # Custom domain-specific rules

  runner/
    execute.py       # Task execution engine
    aggregate.py     # Result aggregation and collection

  report/
    assets/          # HTML, CSS, JS templates
    generate.py      # Report generation
```

## Configuration

### Environment Variables

```bash
# Set in your environment or .env file
export DB_HOST="your-database-host"
export DB_PORT="5432"
export DB_NAME="your-database-name"
export DB_USER="your-username"
export DB_PASSWORD="your-password"
```

## Rule Development

### SQL-Based Rules

```python
from egon_validation.rules.base import SqlRule, RuleResult
from egon_validation.rules.registry import register

@register(task="data_quality", dataset="public.load_profiles",
          rule_id="VALUE_RANGE", kind="formal",
          column="value", min_value=0.0, max_value=1000.0)
class ValueRangeCheck(SqlRule):
    def sql(self, ctx):
        return f"SELECT COUNT(*) as total, COUNT(CASE WHEN {self.params['column']} < {self.params['min_value']} OR {self.params['column']} > {self.params['max_value']} THEN 1 END) as invalid FROM {self.dataset}"

    def postprocess(self, row, ctx):
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=row['invalid'] == 0, observed=row['invalid'], expected=0
        )
```

### Built-in Rules

- `NullCheck` - Validates no NULL values
- `DataTypeCheck` - Validates column data types
- `SRIDUniqueNonZero` - PostGIS SRID validation
- `GeometryCheck` - PostGIS geometry validity
- `ReferentialIntegrityCheck` - Foreign key validation
- `RowCountCheck` - Row count validation
- `ValueSetCheck` - Allowed values validation
- `ArrayCardinalityCheck` - Array length validation

## Testing

```bash
pytest
pytest --cov=egon_validation --cov-report=html
```

## Airflow Integration

```python
from airflow.operators.bash import BashOperator

validation_task = BashOperator(
    task_id='data_validation',
    bash_command='''
        RUNID="{{ ds }}_{{ ts_nodash }}"
        egon-validation run-task --run-id $RUNID --task validation-test
        egon-validation final-report --run-id $RUNID
    '''
)
```

## License

AGPL-3.0