# Compatibility import - rules are now organized in subdirectories
# This file maintains backward compatibility for any existing imports

# Import all formal and sanity rules to ensure they are registered
from .formal import *  # noqa: F403,F401
from .sanity import *   # noqa: F403,F401
