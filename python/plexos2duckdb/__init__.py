"""Python interface for the ``plexos2duckdb`` command-line tool.

This package currently provides a small subprocess-backed wrapper class for
the installed CLI. Native Rust bindings are not exposed at this time.
"""

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
import re

from .api import PLEXOS2DuckDB, PLEXOS2DuckDBError


def _version_from_pyproject() -> str:
    pyproject_path = Path(__file__).resolve().parents[2] / "pyproject.toml"
    if not pyproject_path.is_file():
        raise PackageNotFoundError("plexos2duckdb")

    in_project_section = False
    for line in pyproject_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped == "[project]":
            in_project_section = True
            continue
        if in_project_section and stripped.startswith("["):
            break
        if not in_project_section:
            continue

        match = re.match(r'version\s*=\s*"([^"]+)"', stripped)
        if match:
            return match.group(1)

    raise PackageNotFoundError("plexos2duckdb")


try:
    __version__ = version("plexos2duckdb")
except PackageNotFoundError:
    __version__ = _version_from_pyproject()


__all__ = ["PLEXOS2DuckDB", "PLEXOS2DuckDBError", "__version__"]
