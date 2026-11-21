"""Compatibility import alias. Implementation: ds.failover_controller."""
import importlib as _importlib
import sys as _sys
_sys.modules[__name__] = _importlib.import_module("ds.failover_controller")
