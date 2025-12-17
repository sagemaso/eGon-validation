#!/usr/bin/env python3
"""Example Airflow DAG for eGon validation integration.

This example demonstrates how to integrate the validation framework
into Apache Airflow workflows for automated data quality monitoring.
"""

from datetime import datetime, timedelta
from pathlib import Path

# Airflow imports (commented out for standalone example)
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator


def run_validation_task(**context):
    """Python operator function to run validation programmatically."""
    from egon_validation.context import RunContext
    from egon_validation.runner.execute import run_for_task
    from egon_validation.runner.aggregate import collect
    from egon_validation.report.generate import generate
    from egon_validation import db, config

    # Get execution date from Airflow context
    execution_date = context.get("execution_date", datetime.now())
    run_id = f"airflow-{execution_date.strftime('%Y%m%dT%H%M%S')}"

    # Setup database connection
    db_url = config.get_env("EGON_DB_URL") or config.build_db_url()
    engine = db.make_engine(db_url)

    # Create run context
    ctx = RunContext(run_id=run_id, out_dir=Path("/opt/airflow/validation_runs"))

    # Execute validation
    results = run_for_task(engine, ctx, task="data_quality")

    # Collect and generate reports
    collected_data = collect(ctx)
    generate(ctx)

    # Check if validation passed threshold
    total_rules = len(results)
    failed_rules = sum(1 for r in results if not r.success)
    failure_rate = failed_rules / total_rules if total_rules > 0 else 0

    # Log results for Airflow
    print(f"Validation completed: {run_id}")
    print(f"Total rules: {total_rules}")
    print(f"Failed rules: {failed_rules}")
    print(f"Failure rate: {failure_rate:.1%}")

    # Optionally fail the task if too many validations failed
    max_failure_rate = float(config.get_env("MAX_FAILURE_RATE", "0.1"))  # 10%
    if failure_rate > max_failure_rate:
        raise Exception(
            f"Validation failure rate {failure_rate:.1%} exceeds threshold {max_failure_rate:.1%}"
        )

    return {
        "run_id": run_id,
        "total_rules": total_rules,
        "failed_rules": failed_rules,
        "failure_rate": failure_rate,
    }


# Example DAG definition (uncomment for actual use)
"""
default_args = {
    'owner': 'egon-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'egon_data_validation',
    default_args=default_args,
    description='eGon data quality validation workflow',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    tags=['data-quality', 'egon']
)

# Task 1: Run validation using CLI (alternative approach)
validation_cli_task = BashOperator(
    task_id='run_validation_cli',
    bash_command='''
        RUNID="airflow-{{ ds }}-{{ ts_nodash }}"
        egon-validation run-task --run-id $RUNID --task data_quality --with-tunnel
        egon-validation final-report --run-id $RUNID
        echo "Validation completed: $RUNID"
    ''',
    dag=dag
)

# Task 2: Run validation using Python (programmatic approach)
validation_python_task = PythonOperator(
    task_id='run_validation_python',
    python_callable=run_validation_task,
    dag=dag
)

# Task 3: Optional notification task
def notify_results(**context):
    # Send notification about validation results
    # Could integrate with Slack, email, etc.
    pass

notification_task = PythonOperator(
    task_id='notify_validation_results',
    python_callable=notify_results,
    dag=dag
)

# Define task dependencies
validation_cli_task >> notification_task
# OR
validation_python_task >> notification_task
"""


def main():
    """Standalone execution for testing."""
    import sys
    from unittest.mock import Mock

    # Mock Airflow context for testing
    context = {"execution_date": datetime.now(), "task_instance": Mock(), "dag": Mock()}

    try:
        result = run_validation_task(**context)
        print(f"✅ Validation successful: {result}")
        return 0
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
