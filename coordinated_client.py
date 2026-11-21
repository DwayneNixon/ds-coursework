"""Compatibility import alias. Implementation: ds.coordinated_client."""
import importlib as _importlib
import sys as _sys
_sys.modules[__name__] = _importlib.import_module("ds.coordinated_client")
