"""Public Apixis API and version information."""

from apixis.core import *
from apixis.core import __all__ as _core_exports


# Global configuration settings for Apixis.
VERSION = "0.0.1"

__all__ = [*_core_exports, "VERSION"]
