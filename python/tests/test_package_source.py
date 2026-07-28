"""
Sanity checks that the imported ``plexos2duckdb`` package is this repo's source.
"""

import importlib
import pathlib

import plexos2duckdb


# `python/tests/test_package_source.py` -> parents[1] == `python/`.
REPO_PYTHON_ROOT = pathlib.Path(__file__).resolve().parents[1]
EXPECTED_PACKAGE_DIR = REPO_PYTHON_ROOT / "plexos2duckdb"


def _resolved(module: object) -> pathlib.Path:
    file_attr: str | None = getattr(module, "__file__", None)
    assert file_attr is not None, f"{module!r} has no __file__"
    return pathlib.Path(file_attr).resolve()


def test_package_file_lives_in_this_repo() -> None:
    actual = _resolved(plexos2duckdb)
    expected = EXPECTED_PACKAGE_DIR / "__init__.py"
    assert actual == expected, (
        f"plexos2duckdb was imported from {actual}, expected {expected}. "
        "Something else on sys.path is shadowing this repo's source."
    )


def test_package_path_lives_in_this_repo() -> None:
    package_paths = [pathlib.Path(entry).resolve() for entry in plexos2duckdb.__path__]
    assert package_paths == [EXPECTED_PACKAGE_DIR], (
        f"plexos2duckdb.__path__ resolved to {package_paths}, "
        f"expected [{EXPECTED_PACKAGE_DIR}]."
    )


def test_api_submodule_lives_in_this_repo() -> None:
    # Import lazily so a foreign top-level package without ``api`` still lets
    # the two path checks above surface a helpful assertion message rather
    # than an ImportError at collection time.
    api = importlib.import_module("plexos2duckdb.api")
    actual = _resolved(api)
    expected = EXPECTED_PACKAGE_DIR / "api.py"
    assert actual == expected, (
        f"plexos2duckdb.api was imported from {actual}, expected {expected}."
    )
