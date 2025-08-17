import time, sys
from egon_validation.cli import main

run_id = f"adhoc-{time.strftime('%Y%m%dT%H%M%S')}"

# 1) run-task mit SSH-Tunnel
sys.argv = [
    "egon-validation",
    "run-task",
    "--run-id", run_id,
    "--task", "adhoc",
    "--with-tunnel",
    # Optional: "--scenario", "2040",
    # Optional: "--out", "./validation_runs",
]
main()

# 2) final-report
sys.argv = [
    "egon-validation",
    "final-report",
    "--run-id", run_id,
    # Optional: "--out", "./validation_runs",
]
main()

print("RUN_ID =", run_id)
