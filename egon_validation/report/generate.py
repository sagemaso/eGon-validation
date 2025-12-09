import os
import shutil
import datetime
from egon_validation import __version__


def _replace_tokens(s: str, **tokens) -> str:
    for k, v in tokens.items():
        s = s.replace("{{" + k + "}}", str(v))
    return s


def generate(ctx, base_dir: str = None):
    if base_dir is not None:
        base = base_dir
    else:
        # Generate timestamp for output directory
        task_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        # Handle case when ctx.out_dir is None
        out_dir = ctx.out_dir if ctx.out_dir is not None else "validation_runs"

        # Create timestamped final directory
        base = os.path.join(out_dir, ctx.run_id, f"final.{task_timestamp}")

    os.makedirs(base, exist_ok=True)
    assets = os.path.join(os.path.dirname(__file__), "assets")

    # Copy css/js as-is
    for name in ["report.css", "report.js"]:
        shutil.copy2(os.path.join(assets, name), os.path.join(base, name))

    # Prepare HTML with a few tokens
    with open(os.path.join(assets, "report.html"), "r", encoding="utf-8") as f:
        html = f.read()

    html = _replace_tokens(
        html,
        TITLE="eGon Validation — Report",
        RUN_ID=ctx.run_id,
        GENERATED_AT=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        VERSION=__version__,
    )

    with open(os.path.join(base, "report.html"), "w", encoding="utf-8") as f:
        f.write(html)

    return base
