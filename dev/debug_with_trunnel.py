import time, sys
from egon_validation.cli import main

# Define the task name
task_name = "validation-test"
run_id = f"{task_name}-{time.strftime('%Y%m%dT%H%M%S')}"

# 1) run task with SSH-Tunnel
sys.argv = [
    "egon-validation",
    "run-task",
    "--run-id", run_id,
    "--task", task_name,
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

# 3) final-reporter
sys.argv = [
    "egon-validation",
    "final-reporter",
    "--run-id", run_id,
    # Optional: "--out", "./validation_runs",
]
main()

print("RUN_ID =", run_id)
