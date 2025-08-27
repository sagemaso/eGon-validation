import time, sys
from egon_validation.cli import main

run_id = f"adhoc-{time.strftime('%Y%m%dT%H%M%S')}"

# 1) run adhoc task with SSH-Tunnel
sys.argv = [
    "egon-validation",
    "run-task",
    "--run-id", run_id,
    "--task", "adhoc",
    "--with-tunnel",
]
main()

# 2) run sanity task with SSH-Tunnel
#sys.argv = [
#    "egon-validation",
#    "run-task",
#    "--run-id", run_id,
#"--task", "sanity",
#   "--with-tunnel",
#]
#main()

# 3) final-report
sys.argv = [
    "egon-validation",
    "final-report",
    "--run-id", run_id,
    # Optional: "--out", "./validation_runs",
]
main()

print("RUN_ID =", run_id)
