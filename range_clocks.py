"""Compatibility import alias. Implementation: ds.range_clocks."""
import importlib as _importlib
import sys as _sys
_sys.modules[__name__] = _importlib.import_module("ds.range_clocks")
