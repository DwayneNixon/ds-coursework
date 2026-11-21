"""Compatibility import alias. Implementation: ds.parameter_client."""
import importlib as _importlib
import sys as _sys
_sys.modules[__name__] = _importlib.import_module("ds.parameter_client")
