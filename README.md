# eGon Validation

> Data quality framework for the eGon energy system data pipeline

[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL%203.0-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Version](https://img.shields.io/badge/version-1.1.1-green.svg)](https://github.com/yourusername/egon-validation)

A SQL-first validation framework for PostgreSQL/PostGIS databases, designed for large-scale energy system data pipelines. Execute validation rules directly in the database, generate interactive reports, and integrate seamlessly with Airflow workflows.

## Features

- **SQL-First Execution** - Push validation logic to the database for optimal performance
- **PostGIS Support** - Native geometry and SRID validation for spatial data
- **Extensible Rules** - Combine built-in formal rules with custom domain logic
- **Rich Reports** - Interactive HTML reports with filtering and coverage analysis
- **Airflow Ready** - Resume-safe execution with unique run tracking
- **Parallel Processing** - Thread-safe multi-rule execution

## Quick Start

### Installation

```bash
pip install -e .

# With test dependencies
pip install -e ".[test]"
```

### Basic Usage

1. **Configure database connection:**

```bash
export DB_URL="postgresql://user:password@host:port/database"
```

2. **Run validation:**

```bash
# Generate run ID
RUNID="validation-$(date +%Y%m%dT%H%M%S)"

# Execute validation rules
egon-validation run-task --run-id $RUNID --task validation-test

# Generate HTML report
egon-validation final-report --run-id $RUNID
```

3. **View results:**

```bash
open validation_runs/$RUNID/final/report.html
```

## Writing Rules

### SQL Rule

```python
from egon_validation.rules.base import SqlRule, RuleResult
from egon_validation.rules.registry import register

@register(
    task="data_quality",
    dataset="public.generators",
    rule_id="CAPACITY_RANGE",
    kind="formal",
    column="capacity_mw",
    min_val=0,
    max_val=10000
)
class CapacityRangeCheck(SqlRule):
    def sql(self, ctx):
        col = self.params['column']
        min_v = self.params['min_val']
        max_v = self.params['max_val']

        return f"""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN {col} < {min_v} OR {col} > {max_v}
                      THEN 1 END) as invalid
            FROM {self.dataset}
        """

    def postprocess(self, row, ctx):
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            dataset=self.dataset,
            success=row['invalid'] == 0,
            observed=row['invalid'],
            expected=0
        )
```

### Built-in Rules

| Rule | Purpose |
|------|---------|
| `NotNullAndNotNaNValidation` | Validates no NULL/NaN values in one or more specified columns |
| `WholeTableNotNullAndNotNaNValidation` | Validates no NULL/NaN values in all table columns (auto-discovery) |
| `DataTypeValidation` | Verifies data types for one or more columns |
| `GeometryContainmentValidation` | PostGIS geometry validity and containment |
| `SRIDUniqueNonZero` | PostGIS SRID consistency (unique, non-zero) |
| `SRIDSpecificValidation` | PostGIS SRID validation against expected value |
| `ReferentialIntegrityValidation` | Foreign key validation |
| `RowCountValidation` | Row count boundaries |
| `ValueSetValidation` | Enum/allowed values |
| `ArrayCardinalityValidation` | Array length constraints |

## Configuration

Configure via environment variables or `.env` file:

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=egon-data
DB_USER=postgres
DB_PASS=secret

# SSH Tunnel (optional)
SSH_HOST=gateway.example.com
SSH_USER=username
SSH_KEY_FILE=~/.ssh/id_rsa

# Execution
MAX_WORKERS=6
OUTPUT_DIR=./validation_runs
DEFAULT_TOLERANCE=0.0
```

## Airflow Integration

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('data_validation', start_date=datetime(2024, 1, 1)) as dag:

    validate = BashOperator(
        task_id='validate_data',
        bash_command='''
            RUNID="{{ ds }}_{{ ts_nodash }}"
            egon-validation run-task --run-id $RUNID --task validation-test --with-tunnel
            egon-validation final-report --run-id $RUNID
        '''
    )
```

## Project Structure

```
egon_validation/
├── cli.py                 # Command-line interface
├── config.py              # Configuration management
├── context.py             # Run tracking
├── db.py                  # Database connections
├── rules/
│   ├── base.py           # Base rule classes
│   ├── registry.py       # Rule registration
│   ├── formal/           # Built-in rules
│   └── custom/           # Domain-specific rules
├── runner/
│   ├── execute.py        # Task execution
│   └── aggregate.py      # Result aggregation
└── report/
    ├── generate.py       # HTML report generation
    └── assets/           # Report templates
```

## Development

```bash
# Run tests
pytest

# With coverage
pytest --cov=egon_validation --cov-report=html

# Format code
black egon_validation/

# Lint
flake8 egon_validation/
```

## License

AGPL-3.0 - see [LICENSE](LICENSE)

## Contributing

Contributions welcome! Please ensure:
- Tests pass (`pytest`)
- Code is formatted (`black`)
- New rules include examples and tests