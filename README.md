# eGon Validation

> Data quality framework for the eGon energy system data pipeline

[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL%203.0-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Version](https://img.shields.io/badge/version-1.1.1-green.svg)](https://github.com/yourusername/egon-validation)

SQL-first validation framework for PostgreSQL/PostGIS databases. Execute validation rules directly in the database, generate interactive reports, and integrate with Airflow workflows.

## Features

- **SQL-First Execution** - Push validation logic to the database
- **PostGIS Support** - Geometry and SRID validation
- **Extensible Rules** - Built-in + custom rules
- **HTML Reports** - Interactive reports with filtering
- **Airflow Ready** - Pipeline integration
- **Parallel Processing** - Multi-threaded execution

## Quick Start

```bash
pip install -e .

export DB_URL="postgresql://user:password@host:port/database"

egon-validation run-task --run-id my-run --task validation-test
egon-validation final-report --run-id my-run
```

Output: `validation_runs/my-run/final/report.html`

## Documentation

See [docs/](docs/) for full documentation:

- [Installation & Configuration](docs/installation.md)
- [CLI Reference](docs/cli.md)
- [Built-in Rules](docs/rules.md)
- [Custom Rules](docs/custom-rules.md)
- [Pipeline Integration](docs/pipeline-integration.md)

## Project Structure

```
egon_validation/
├── cli.py                 # Command-line interface
├── config.py              # Configuration management
├── db.py                  # Database connections
├── rules/
│   ├── base.py            # Base rule classes
│   ├── formal/            # Built-in rules
│   └── custom/            # Domain-specific rules
├── runner/
│   └── execute.py         # Task execution
└── report/
    └── generate.py        # HTML report generation
```

## Development

```bash
pytest                                    # Run tests
pytest --cov=egon_validation             # With coverage
black egon_validation/                    # Format
flake8 egon_validation/                   # Lint
```

## License

AGPL-3.0 - see [LICENSE](LICENSE)