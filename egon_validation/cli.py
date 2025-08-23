import argparse, os, json, datetime
from egon_validation.config import DEFAULT_OUT_DIR, ENV_DB_URL, get_env, build_db_url
from egon_validation.context import RunContext
from egon_validation.db import make_engine
from egon_validation.runner.execute import run_for_task
from egon_validation.runner.coverage_analysis import discover_total_tables
from egon_validation.runner.aggregate import collect, build_coverage, write_outputs
from egon_validation.report.generate import generate
from egon_validation.ssh_tunnel import create_tunnel_from_env
import egon_validation.rules.formal  # noqa: F401
import egon_validation.rules.custom.sanity   # noqa: F401
import egon_validation.rules.custom  # noqa: F401


def _save_table_count(ctx, total_tables):
    """Save table count to metadata file for use in final report"""
    tasks_dir = os.path.join(ctx.out_dir, ctx.run_id, "tasks")
    os.makedirs(tasks_dir, exist_ok=True)
    metadata_file = os.path.join(tasks_dir, "db_metadata.json")
    metadata = {"total_tables": total_tables}
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=2)


def _run_task(args):
    db_url = args.db_url or get_env(ENV_DB_URL) or build_db_url()
    if not db_url:
        raise SystemExit("Missing DB URL (use --db-url, set EGON_DB_URL, or configure .env file)")
    
    ctx = RunContext(run_id=args.run_id, scenario=None, out_dir=args.out, extra={})
    
    # Use SSH tunnel if configured and --with-tunnel flag is set
    if args.with_tunnel and all([get_env("SSH_HOST"), get_env("SSH_USER"), get_env("SSH_KEY_FILE")]):
        print("Starting SSH tunnel...")
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
    
    print(f"Written task results for '{args.task}' -> {os.path.join(ctx.out_dir, ctx.run_id, 'tasks', args.task)}")

def _final_report(args):
    ctx = RunContext(run_id=args.run_id, scenario=None, out_dir=args.out, extra={})
    collected = collect(ctx)
    coverage = build_coverage(ctx, collected)
    out_dir = write_outputs(ctx, collected, coverage)
    generate(ctx)
    print(f"Final report at: {os.path.join(out_dir, 'report.html')}")

def main():
    p = argparse.ArgumentParser(prog="egon-validation", description="eGon validation (dev CLI)")
    subs = p.add_subparsers(dest="cmd", required=True)

    p1 = subs.add_parser("run-task", help="Run rules for a single task")
    p1.add_argument("--db-url", type=str, help="PostgreSQL URL (or set EGON_DB_URL or configure .env)")
    p1.add_argument("--run-id", required=True, type=str)
    p1.add_argument("--task", required=True, type=str)
    p1.add_argument("--out", type=str, default=DEFAULT_OUT_DIR)
    p1.add_argument("--with-tunnel", action="store_true", help="Use SSH tunnel (requires SSH config in .env)")
    p1.add_argument("--echo-sql", action="store_true", help="Echo SQLAlchemy SQL for debugging")
    p1.set_defaults(func=_run_task)

    p2 = subs.add_parser("final-report", help="Aggregate results and write final report")
    p2.add_argument("--run-id", required=True, type=str)
    p2.add_argument("--out", type=str, default=DEFAULT_OUT_DIR)
    p2.add_argument("--list-rules", action="store_true", help="Print registered rules before building report")
    p2.set_defaults(func=_final_report)

    args = p.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
