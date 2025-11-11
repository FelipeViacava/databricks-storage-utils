from __future__ import annotations
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("databricks-storage-utils")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"

from .core import *  # re-export everything from core