"""Compatibility import alias. Implementation: ds.server_manager."""
import importlib as _importlib
import sys as _sys
_sys.modules[__name__] = _importlib.import_module("ds.server_manager")
