"""Compatibility import and command-line entrypoint for ds.server."""
if __name__ == "__main__":
    import runpy as _runpy
    _runpy.run_module("ds.server", run_name="__main__")
else:
    import importlib as _importlib
    import sys as _sys
    _sys.modules[__name__] = _importlib.import_module("ds.server")
