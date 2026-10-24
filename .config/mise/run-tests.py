# /// script
# requires-python = ">=3.12"
# ///
"""Run both test suites against this checkout and its freshly built CLI."""

import json
import os
import pathlib
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[2]


def cli_executable(build_output: str) -> pathlib.Path:
    executables = {
        pathlib.Path(message["executable"])
        for line in build_output.splitlines()
        if line.strip()
        and (message := json.loads(line)).get("reason") == "compiler-artifact"
        and message["target"]["name"] == "plexos2duckdb"
        and "bin" in message["target"]["kind"]
        and message.get("executable")
    }
    if len(executables) != 1:
        raise ValueError("Cargo did not report exactly one plexos2duckdb executable")
    return executables.pop()


def run_tests(root: pathlib.Path) -> None:
    subprocess.run(["cargo", "test", "--locked"], cwd=root, check=True)
    # Cargo reports the real path, including custom target directories and targets.
    build = subprocess.run(
        [
            "cargo",
            "build",
            "--locked",
            "--bin",
            "plexos2duckdb",
            "--message-format=json-render-diagnostics",
        ],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    executable = cli_executable(build.stdout)
    if not executable.is_absolute():
        executable = root / executable
    if not executable.is_file():
        raise ValueError(f"Built CLI does not exist: {executable}")

    # Use the project's environment, not uv's enclosing script environment.
    env = os.environ.copy()
    env.pop("VIRTUAL_ENV", None)
    subprocess.run(
        ["uv", "sync", "--directory", "python", "--locked", "--no-install-project"],
        cwd=root,
        env=env,
        check=True,
    )
    env["PLEXOS2DUCKDB_TEST_EXECUTABLE"] = str(executable)
    python_paths = [str(root / "python")]
    if env.get("PYTHONPATH"):
        python_paths.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(python_paths)
    subprocess.run(
        [
            "uv", "run", "--directory", "python", "--locked", "--no-sync",
            "python", "-m", "pytest",
        ],
        cwd=root,
        env=env,
        check=True,
    )


def main() -> int:
    try:
        run_tests(ROOT)
    except subprocess.CalledProcessError as error:
        return error.returncode
    except (OSError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
