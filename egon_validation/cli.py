import argparse, os, json, datetime
from egon_validation.config import DEFAULT_OUT_DIR, ENV_DB_URL, get_env
from egon_validation.context import RunContext
from egon_validation.db import make_engine
from egon_validation.runner.execute import run_for_task
from egon_validation.runner.aggregate import collect, build_coverage, write_outputs
from egon_validation.report.generate import generate
import egon_validation.rules.formal  # noqa: F401
import egon_validation.rules.custom  # noqa: F401


def _run_task(args):
    db_url = args.db_url or get_env(ENV_DB_URL)
    if not db_url:
        raise SystemExit("Missing DB URL (use --db-url or set EGON_DB_URL)")
    ctx = RunContext(run_id=args.run_id, scenario=args.scenario, out_dir=args.out, extra={})
    engine = make_engine(db_url)
    run_for_task(engine, ctx, args.task)
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
    p1.add_argument("--db-url", type=str, help="PostgreSQL URL (or set EGON_DB_URL)")
    p1.add_argument("--run-id", required=True, type=str)
    p1.add_argument("--task", required=True, type=str)
    p1.add_argument("--scenario", type=str, default=None)
    p1.add_argument("--out", type=str, default=DEFAULT_OUT_DIR)
    p1.set_defaults(func=_run_task)

    p2 = subs.add_parser("final-report", help="Aggregate results and write final report")
    p2.add_argument("--run-id", required=True, type=str)
    p2.add_argument("--out", type=str, default=DEFAULT_OUT_DIR)
    p2.set_defaults(func=_final_report)

    args = p.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
