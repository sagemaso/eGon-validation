import argparse
import json
import os
from egon_validation.config import DEFAULT_OUT_DIR, ENV_DB_URL, get_env, build_db_url
from egon_validation.context import RunContext
from egon_validation.db import make_engine
from egon_validation.runner.execute import run_for_task
from egon_validation.runner.coverage_analysis import discover_total_tables
from egon_validation.runner.aggregate import collect, build_coverage, write_outputs
from egon_validation.report.generate import generate
from egon_validation.ssh_tunnel import create_tunnel_from_env
from egon_validation.logging_config import setup_logging, get_logger
import egon_validation.rules.formal  # noqa: F401
import egon_validation.rules.custom  # noqa: F401

# Setup logging
logger = get_logger("cli")


def _save_table_count(ctx, total_tables):
    """Save table count to metadata file for use in final report"""
    tasks_dir = os.path.join(ctx.out_dir, ctx.run_id, "tasks")
    os.makedirs(tasks_dir, exist_ok=True)
    metadata_file = os.path.join(tasks_dir, "db_metadata.json")
    metadata = {"total_tables": total_tables}
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=2)


def _run_task(args):
    try:
        db_url = args.db_url or get_env(ENV_DB_URL) or build_db_url()
        if not db_url:
            logger.error("No database URL provided", extra={
                "available_methods": ["--db-url", "EGON_DB_URL env var", ".env file"]
            })
            raise SystemExit("Missing DB URL (use --db-url, set EGON_DB_URL, or configure .env file)")

        ctx = RunContext(run_id=args.run_id, out_dir=args.out, extra={})
        logger.info("Starting validation task", extra={
            "task": args.task,
            "run_id": args.run_id,
            "output_dir": args.out,
            "with_tunnel": args.with_tunnel
        })

        # Use SSH tunnel if configured and --with-tunnel flag is set
        if args.with_tunnel and all([get_env("SSH_HOST"), get_env("SSH_USER"), get_env("SSH_KEY_FILE")]):
            logger.info("Starting SSH tunnel for database connection")
            with create_tunnel_from_env():
                engine = make_engine(db_url, echo=args.echo_sql)
                try:
                    run_for_task(engine, ctx, args.task)
                    # Capture table count while DB is accessible
                    total_tables = discover_total_tables()
                    _save_table_count(ctx, total_tables)
                finally:
                    engine.dispose()
        else:
            engine = make_engine(db_url, echo=args.echo_sql)
            try:
                run_for_task(engine, ctx, args.task)
                # Capture table count while DB is accessible
                total_tables = discover_total_tables()
                _save_table_count(ctx, total_tables)
            finally:
                engine.dispose()

        output_path = os.path.join(ctx.out_dir, ctx.run_id, 'tasks', args.task)
        logger.info("Task completed successfully", extra={
            "task": args.task,
            "output_path": output_path,
            "run_id": args.run_id
        })


    except KeyboardInterrupt:
        logger.warning("Task interrupted by user", extra={"task": args.task, "run_id": args.run_id})
        raise SystemExit(1)
    except Exception as e:
        logger.error("Task execution failed", extra={
            "task": args.task,
            "run_id": args.run_id,
            "error": str(e),
            "error_type": type(e).__name__
        })
        raise SystemExit(f"Task failed: {e}")

def _final_report(args):
    try:
        ctx = RunContext(run_id=args.run_id, scenario=None, out_dir=args.out, extra={})
        logger.info("Starting final report generation", extra={
            "run_id": args.run_id,
            "output_dir": args.out
        })

        collected = collect(ctx)
        coverage = build_coverage(ctx, collected)
        out_dir = write_outputs(ctx, collected, coverage)
        generate(ctx)

        report_path = os.path.join(out_dir, 'report.html')
        logger.info("Final report generated successfully", extra={
            "report_path": report_path,
            "run_id": args.run_id,
            "total_results": len(collected.get("items", []))
        })

    except FileNotFoundError as e:
        logger.error("Required validation data not found", extra={
            "run_id": args.run_id,
            "error": str(e),
            "suggestion": "Ensure run-task was completed successfully first"
        })
        raise SystemExit(f"Report generation failed: {e}")
    except Exception as e:
        logger.error("Final report generation failed", extra={
            "run_id": args.run_id,
            "error": str(e),
            "error_type": type(e).__name__
        })
        raise SystemExit(f"Report generation failed: {e}")


def main():
    setup_logging()
    p = argparse.ArgumentParser(
        prog="egon-validation", description="eGon validation (dev CLI)"
    )
    subs = p.add_subparsers(dest="cmd", required=True)

    p1 = subs.add_parser("run-task", help="Run rules for a single task")
    p1.add_argument(
        "--db-url",
        type=str,
        help="PostgreSQL URL (or set EGON_DB_URL or configure .env)",
    )
    p1.add_argument("--run-id", required=True, type=str)
    p1.add_argument("--task", required=True, type=str)
    p1.add_argument("--out", type=str, default=DEFAULT_OUT_DIR)
    p1.add_argument("--with-tunnel", action="store_true", help="Use SSH tunnel (requires SSH config in .env)")
    p1.add_argument("--echo-sql", action="store_true", help="Echo SQLAlchemy SQL for debugging")
    p1.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Set logging level")
    p1.add_argument("--log-format", type=str, default="pipeline", choices=["pipeline", "dev"], help="Set log format style")
    p1.set_defaults(func=_run_task)

    p2 = subs.add_parser("final-report", help="Aggregate results and write final report")
    p2.add_argument("--run-id", required=True, type=str)
    p2.add_argument("--out", type=str, default=DEFAULT_OUT_DIR)
    p2.add_argument("--list-rules", action="store_true", help="Print registered rules before building report")
    p2.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Set logging level")
    p2.add_argument("--log-format", type=str, default="pipeline", choices=["pipeline", "dev"], help="Set log format style")
    p2.set_defaults(func=_final_report)

    args = p.parse_args()

    # Initialize logging with CLI arguments or environment variables
    log_level = getattr(args, 'log_level', None) or os.getenv("EGON_LOG_LEVEL", "INFO")
    log_format = getattr(args, 'log_format', None) or os.getenv("EGON_LOG_FORMAT", "pipeline")
    setup_logging(level=log_level, format_style=log_format)

    args.func(args)


if __name__ == "__main__":
    main()
