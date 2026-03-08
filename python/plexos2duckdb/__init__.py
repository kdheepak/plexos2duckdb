"""Python interface for the ``plexos2duckdb`` command-line tool.

This package currently provides a small subprocess-backed wrapper class for
the installed CLI. Native Rust bindings are not exposed at this time.
"""

from .api import PLEXOS2DuckDB, PLEXOS2DuckDBError

__all__ = ["PLEXOS2DuckDB", "PLEXOS2DuckDBError"]
