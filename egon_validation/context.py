from dataclasses import dataclass
from typing import Optional, Dict, Any

@dataclass
class RunContext:
    run_id: str
    out_dir: str = "validation_runs"
    extra: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.extra is None:
            self.extra = {}
