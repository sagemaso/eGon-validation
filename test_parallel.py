#!/usr/bin/env python3

import time, sys
from egon_validation.cli import main

run_id = f"test-parallel-{time.strftime('%Y%m%dT%H%M%S')}"

print("Testing parallel execution with connection pooling...")
print(f"Run ID: {run_id}")

start_time = time.time()

sys.argv = [
    "egon-validation",
    "run-task",
    "--run-id", run_id,
    "--task", "adhoc",
    "--with-tunnel",
]

main()

total_time = time.time() - start_time
print(f"\nTotal execution time: {total_time:.2f}s")
print("RUN_ID =", run_id)