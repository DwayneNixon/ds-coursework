"""Compatibility import alias. Implementation: ds.worker_heartbeat."""
import importlib as _importlib
import sys as _sys
_sys.modules[__name__] = _importlib.import_module("ds.worker_heartbeat")
