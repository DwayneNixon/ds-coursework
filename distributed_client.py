"""Compatibility import alias. Implementation: ds.distributed_client."""
import importlib as _importlib
import sys as _sys
_sys.modules[__name__] = _importlib.import_module("ds.distributed_client")
