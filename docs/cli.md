# CLI Reference

## run-task

Execute validation rules for a task.

```bash
egon-validation run-task --run-id <ID> --task <TASK> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--run-id` | Unique identifier for this run (required) |
| `--task` | Task name to execute (required) |
| `--db-url` | Database URL (or use env var) |
| `--out` | Output directory (default: `./validation_runs`) |
| `--with-tunnel` | Use SSH tunnel from env config |
| `--echo-sql` | Print SQL queries for debugging |

Example:
```bash
egon-validation run-task --run-id validation-20260116 --task data_quality
```

## final-report

Aggregate results and generate HTML report.

```bash
egon-validation final-report --run-id <ID> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--run-id` | Run ID to aggregate (required) |
| `--out` | Output directory (default: `./validation_runs`) |
| `--list-rules` | Print registered rules |

Example:
```bash
egon-validation final-report --run-id validation-20260116
```

Output: `validation_runs/<run-id>/final/report.html`