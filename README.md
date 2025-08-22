# egon-validation (skeleton)

Validation library for the eGon data pipeline.

- Python 3.8
- Rules **hard-coded** in Python (no CSV)
- SQL-first execution (fast; minimal RAM)
- Reports as static **HTML + CSS + JS** (clean separation)
- **Default tolerance = 0.0** (absolute)
- **Severity = WARNING** by default
- Does **not** fail the DAG; produces reports only
- Resume-safe via **run_id**
- Scenario-aware rules (`scenario` + per-rule `scenario_col`)

## Quickstart

### Option 1: Debug Script (Recommended)

```bash
# Install (editable)
pip install -e .

# Configure .env file for database connection
# Contact your system administrator for database configuration details

# Run validation using the debug script
python3 dev/debug_with_trunnel.py
```

The debug script automatically:
- Generates a timestamped run ID
- Runs the adhoc task with proper database connection
- Generates the final report
- Prints the run ID for reference

### Option 2: Manual CLI Commands

```bash
# Run validation (step-by-step)
RUNID="adhoc-$(date +%Y%m%dT%H%M%S)"
python3 -c "from egon_validation.cli import main; import sys; sys.argv = ['egon-validation', 'run-task', '--run-id', '$RUNID', '--task', 'adhoc', '--with-tunnel']; main()"
python3 -c "from egon_validation.cli import main; import sys; sys.argv = ['egon-validation', 'final-report', '--run-id', '$RUNID']; main()"

# Alternative: Direct database connection (requires database URL)
RUNID="direct-$(date +%Y%m%dT%H%M%S)"
python3 -c "from egon_validation.cli import main; import sys; sys.argv = ['egon-validation', 'run-task', '--db-url', 'DATABASE_URL_HERE', '--run-id', '$RUNID', '--task', 'adhoc']; main()"
python3 -c "from egon_validation.cli import main; import sys; sys.argv = ['egon-validation', 'final-report', '--run-id', '$RUNID']; main()"
```

Open `./validation_runs/<run_id>/final/report.html` (requires `report.css`, `report.js`, `results.json`, `coverage.json` in the same folder).

For a single-file version, you can later add a flag to inline CSS/JS/data.

## Structure

```
egon_validation/
  config.py          # .env/ENV helpers, defaults
  context.py         # RunContext(run_id, scenario, out_dir, extra)
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

## Add a rule

```python
# rules/formal.py
from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

@register(task="build_lp", dataset="public.load_profiles", rule_id="LP_NOT_NULL_NAN",
          kind="formal", column="value", scenario_col=None)
class NotNullAndNotNaN(SqlRule):
    def sql(self, ctx):
        col = self.params.get("column", "value")
        scenario_col = self.params.get("scenario_col")
        where = f"WHERE ({col} IS NULL OR {col} <> {col})"
        if ctx.scenario and scenario_col:
            where += f" AND {scenario_col} = :scenario"
        return f"SELECT COUNT(*) AS n_bad FROM {self.dataset} {where}"

    def postprocess(self, row, ctx):
        n_bad = int(row["n_bad"] or 0)
        ok = (n_bad == 0)
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, message=f"{n_bad} offending rows (NULL/NaN)",
            severity=Severity.WARNING, schema=self.schema, table=self.table, column=self.params.get("column")
        )
```