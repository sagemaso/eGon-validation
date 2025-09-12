#!/usr/bin/env python3
"""Basic usage example for eGon validation framework.

This example demonstrates how to:
1. Set up database connection
2. Create a validation run
3. Execute validation rules
4. Generate reports
"""

import os
from pathlib import Path
from egon_validation.context import RunContext
from egon_validation.runner.execute import run_for_task
from egon_validation.runner.aggregate import collect
from egon_validation.report.generate import generate
from egon_validation import db, config


def main():
    """Run basic validation example."""

    # 1. Setup database connection
    db_url = config.get_env("DB_URL") or config.build_db_url()
    if not db_url:
        print("❌ Database connection not configured")
        print("Set DB_URL or individual DB_* environment variables")
        return

    engine = db.make_engine(db_url)
    print(f"✅ Connected to database")

    # 2. Create run context
    run_id = f"example-{int(__import__('time').time())}"
    ctx = RunContext(run_id=run_id, out_dir=Path("validation_runs"))
    print(f"🔄 Starting validation run: {run_id}")

    # 3. Execute validation rules
    try:
        results = run_for_task(engine, ctx, "adhoc")
        print(f"✅ Executed {len(results)} validation rules")

        # 4. Collect and aggregate results
        collected_data = collect(ctx)
        print(
            f"📊 Collected results for {len(collected_data.get('datasets', []))} datasets"
        )

        # 5. Generate HTML report
        report_dir = generate(ctx)
        report_path = Path(report_dir) / "report.html"
        print(f"📄 Report generated: {report_path}")

        # 6. Show summary
        total_rules = len(collected_data.get("items", []))
        successful = sum(
            1 for item in collected_data.get("items", []) if item.get("success")
        )
        failed = total_rules - successful

        print(f"\n📈 Validation Summary:")
        print(f"   Total rules: {total_rules}")
        print(f"   ✅ Passed: {successful}")
        print(f"   ❌ Failed: {failed}")
        print(f"   Success rate: {(successful/total_rules*100):.1f}%")

    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
