# Auto-import all modules in this package so their @register / register_map runs.
import importlib
import pkgutil
from pathlib import Path

_pkg_path = Path(__file__).parent
for mod in pkgutil.iter_modules([str(_pkg_path)]):
    # skip subpackages and private modules
    if mod.ispkg or mod.name.startswith("_"):
        continue
    importlib.import_module(f"{__name__}.{mod.name}")
