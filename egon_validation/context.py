from dataclasses import dataclass
from typing import Optional, Dict, Any

@dataclass
class RunContext:
    run_id: str
    scenario: Optional[str]
    out_dir: str
    extra: Dict[str, Any]
