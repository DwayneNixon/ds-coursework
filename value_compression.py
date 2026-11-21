"""Compatibility import alias. Implementation: ds.value_compression."""
import importlib as _importlib
import sys as _sys
_sys.modules[__name__] = _importlib.import_module("ds.value_compression")
