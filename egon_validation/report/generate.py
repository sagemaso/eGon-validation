import os
import shutil
import datetime


def _replace_tokens(s: str, **tokens) -> str:
    for k, v in tokens.items():
        s = s.replace("{{" + k + "}}", str(v))
    return s


def generate(ctx, version: str = "0.1.0"):
    base = os.path.join(ctx.out_dir, ctx.run_id, "final")
    os.makedirs(base, exist_ok=True)
    assets = os.path.join(os.path.dirname(__file__), "assets")

    # Copy css/js as-is
    for name in ["reporter.css", "reporter.js"]:
        shutil.copy2(os.path.join(assets, name), os.path.join(base, name))

    # Prepare HTML with a few tokens
    with open(os.path.join(assets, "reporter.html"), "r", encoding="utf-8") as f:
        html = f.read()

    html = _replace_tokens(
        html,
        TITLE="eGon Validation — Report",
        RUN_ID=ctx.run_id,
        GENERATED_AT=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        VERSION=version,
    )

    with open(os.path.join(base, "reporter.html"), "w", encoding="utf-8") as f:
        f.write(html)

    return base
