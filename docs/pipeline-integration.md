# Pipeline Integration

## Airflow

### Bash Operator

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("data_validation", start_date=datetime(2024, 1, 1)) as dag:

    validate = BashOperator(
        task_id="validate",
        bash_command='''
            RUNID="{{ ds }}_{{ ts_nodash }}"
            egon-validation run-task --run-id $RUNID --task data_quality --with-tunnel
            egon-validation final-report --run-id $RUNID
        '''
    )
```

### Python Operator

```python
from airflow.operators.python import PythonOperator

def run_validation(**context):
    from egon_validation.context import RunContext
    from egon_validation.runner.execute import run_for_task
    from egon_validation.db import make_engine
    from egon_validation.config import build_db_url

    run_id = f"airflow-{context['ds']}"
    engine = make_engine(build_db_url())
    ctx = RunContext(run_id=run_id)

    results = run_for_task(engine, ctx, task="data_quality")

    # Fail if too many rules failed
    failed = sum(1 for r in results if not r.success)
    if failed / len(results) > 0.1:
        raise Exception(f"{failed} rules failed")

validate = PythonOperator(
    task_id="validate",
    python_callable=run_validation
)
```

## CI/CD (GitHub Actions)

```yaml
name: Data Validation

on:
  schedule:
    - cron: '0 6 * * *'  # Daily at 6 AM

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'

      - name: Install
        run: pip install -e .

      - name: Run Validation
        env:
          DB_URL: ${{ secrets.DATABASE_URL }}
        run: |
          RUNID="ci-${{ github.run_id }}"
          egon-validation run-task --run-id $RUNID --task data_quality
          egon-validation final-report --run-id $RUNID

      - name: Upload Report
        uses: actions/upload-artifact@v4
        with:
          name: validation-report
          path: validation_runs/*/final/report.html
```

## Python Script

```python
#!/usr/bin/env python3
from egon_validation.context import RunContext
from egon_validation.runner.execute import run_for_task
from egon_validation.runner.aggregate import collect, build_coverage, write_outputs
from egon_validation.report.generate import generate
from egon_validation.db import make_engine
from datetime import datetime

# Setup
run_id = f"script-{datetime.now():%Y%m%dT%H%M%S}"
engine = make_engine("postgresql://user:pass@host/db")
ctx = RunContext(run_id=run_id)

# Execute
results = run_for_task(engine, ctx, task="data_quality")

# Report
collected = collect(ctx)
coverage = build_coverage(ctx, collected)
out_dir = write_outputs(ctx, collected, coverage)
generate(ctx, base_dir=out_dir)

print(f"Report: {out_dir}/report.html")
```

## Exit Codes

The CLI exits with code 0 on success. For pipeline failure on validation errors, check results programmatically:

```python
failed = [r for r in results if not r.success]
if failed:
    sys.exit(1)
```

## Inline Rule Instantiation

Define validation rules directly in your pipeline code:

```python
from egon_validation import (
    RowCountValidation,
    DataTypeValidation,
    WholeTableNotNullAndNotNaNValidation,
    run_validations,
    RunContext
)

validation_dict = {
    "data_quality": [
        RowCountValidation(
            table="<schema>.<table_name>",
            rule_id="ROW_COUNT.<table_name>",
            expected_count={"region_a": 7519, "region_b": 35718586}
        ),
        DataTypeValidation(
            table="<schema>.<table_name>",
            rule_id="DATA_TYPES.<table_name>",
            column_types={
                "id": "bigint",
                "name": "text",
                "value": "numeric"
            }
        ),
        WholeTableNotNullAndNotNaNValidation(
            table="<schema>.<table_name>",
            rule_id="NOT_NULL.<table_name>"
        ),
    ]
}
```

## Context-Dependent Parameters

For pipelines with multiple configurations (e.g., different regions), parameters can be dicts resolved at runtime:

```python
RowCountValidation(
    table="<schema>.<table_name>",
    rule_id="ROW_COUNT",
    expected_count={"region_a": 27, "region_b": 537}
)
```

Resolution helper:

```python
def resolve_params(rule, context_key):
    """Resolve dict parameters based on context."""
    for name, value in rule.params.items():
        if isinstance(value, dict) and context_key in value:
            rule.params[name] = value[context_key]
```

## Advanced: Validation Task Factory

For large pipelines, create tasks programmatically:

```python
from airflow.operators.python import PythonOperator
from egon_validation import run_validations, RunContext

def create_validation_tasks(validation_dict, dataset_name, on_failure="continue"):
    """Convert validation rules to Airflow tasks.

    Args:
        validation_dict: {"task_name": [Rule1(), Rule2()]}
        dataset_name: Name for task_id prefix
        on_failure: "continue" or "fail"

    Returns:
        List of PythonOperator tasks
    """
    tasks = []

    for task_name, rules in validation_dict.items():
        def make_callable(rules, task_name):
            def run_validation(**context):
                from your_project import db, config

                run_id = context.get("dag_run").run_id
                engine = db.engine()
                ctx = RunContext(run_id=run_id)

                results = run_validations(engine, ctx, rules, task_name)

                failed = sum(1 for r in results if not r.success)
                if failed > 0 and on_failure == "fail":
                    raise Exception(f"{failed} validations failed")

                return {"total": len(results), "failed": failed}

            return run_validation

        tasks.append(PythonOperator(
            task_id=f"{dataset_name}.validate.{task_name}",
            python_callable=make_callable(rules, task_name),
        ))

    return tasks
```

Usage:
```python
validation_dict = {
    "data_quality": [RowCountValidation(...), NotNullAndNotNaNValidation(...)],
    "geometry": [SRIDValidation(...), GeometryValidation(...)],
}

tasks = create_validation_tasks(validation_dict, "my_dataset")
```
