"""Compatibility import alias. Implementation: ds.manager_rpc."""
import importlib as _importlib
import sys as _sys
_sys.modules[__name__] = _importlib.import_module("ds.manager_rpc")
