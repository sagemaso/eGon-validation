from dataclasses import dataclass, field
from typing import Dict, Any
from pathlib import Path

@dataclass
class RunContext:
    run_id: str
    out_dir: Path = Path("validation_runs")
    extra: Dict[str, Any] = field(default_factory=dict)