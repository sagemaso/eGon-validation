# Import rules from organized subdirectories
# This ensures all rules get registered when the module is imported

# Import formal rules
from . import formal

# Import sanity rules  
from .custom import sanity