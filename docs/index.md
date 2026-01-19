# eGon Validation - User Guide

SQL-first validation framework for PostgreSQL/PostGIS databases.

## Documentation

- [Installation & Configuration](installation.md)
- [CLI Reference](cli.md)
- [Built-in Rules](rules.md)
- [Custom Rules](custom-rules.md)
- [Pipeline Integration](pipeline-integration.md)

## Quick Example

```bash
# Set database connection
export DB_URL="postgresql://user:pass@host:5432/db"

# Run validation
egon-validation run-task --run-id my-run --task validation-test

# Generate report
egon-validation final-report --run-id my-run
```

Output: `validation_runs/my-run/final/report.html`