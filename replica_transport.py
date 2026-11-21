"""Compatibility import alias. Implementation: ds.replica_transport."""
import importlib as _importlib
import sys as _sys
_sys.modules[__name__] = _importlib.import_module("ds.replica_transport")
