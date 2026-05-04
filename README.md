# plexos2duckdb

[![Release](https://img.shields.io/github/v/release/epri-dev/plexos2duckdb)](https://github.com/epri-dev/plexos2duckdb/releases)
[![PyPI](https://img.shields.io/pypi/v/plexos2duckdb)](https://pypi.org/project/plexos2duckdb/)
[![License](https://img.shields.io/github/license/epri-dev/plexos2duckdb)](https://github.com/epri-dev/plexos2duckdb/blob/main/LICENSE)
[![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20MacOS%20%7C%20Linux-blue)]()
[![Downloads](https://img.shields.io/github/downloads/epri-dev/plexos2duckdb/total?color=brightgreen)](https://github.com/epri-dev/plexos2duckdb/releases)

> [!IMPORTANT]
>
> This software is in **beta pre-production**. Interfaces, behavior, and data format may change
> without notice before the production release.

`plexos2duckdb` is a software tool to convert PLEXOS solution files to a DuckDB database.

<img width="1190" height="425" alt="image" src="https://github.com/user-attachments/assets/894d1cbb-03b5-40fd-8f1e-da41fd1b29c7" />

## Installation

### Option 1: Install with uv (recommended)

On supported platforms, you can install the command-line tool directly from PyPI using
[`uv`](https://github.com/astral-sh/uv):

```shell
uv add --prerelease=allow plexos2duckdb
# or
uv pip install --prerelease=allow plexos2duckdb
```

Then verify the install:

```shell
plexos2duckdb --version
```

Note that this requires a published wheel for your platform.

If you are interested just in the CLI, you can use `uvx` to install in an isolated environment:

```shell
$ uvx --prerelease=allow plexos2duckdb --help

A tool to convert PLEXOS Solution files to a DuckDB database.

Usage: plexos2duckdb <COMMAND>

Commands:
  convert                     Convert a PLEXOS solution file to DuckDB
  inspect                     Show operational metadata from a generated DuckDB database
  generate-shell-completions  Generate shell completion scripts
  help                        Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

### Option 2: Download pre-built binary

Pre-built binaries for the CLI are available from the
[GitHub Releases](https://github.com/epri-dev/plexos2duckdb/releases) page.

1. Go to the [latest release](https://github.com/epri-dev/plexos2duckdb/releases/latest) page on
   GitHub.
2. Download the appropriate binary for your operating system.
3. Extract the archive:

   ```shell
   tar -xzf plexos2duckdb-<platform>.tar.gz
   ```

   or on Windows:

   ```powershell
   Expand-Archive plexos2duckdb-<platform>.zip -DestinationPath .
   ```

4. Copy the binary to a directory in your `PATH`, e.g.:

   ```shell
   # MacOS/Linux
   cp plexos2duckdb ~/local/bin/
   # Windows
   copy plexos2duckdb.exe %USERPROFILE%\local\bin\
   ```

   You will have to make sure `~/local/bin/` is in your `PATH`.

## Quickstart

### CLI usage

> [!NOTE]
>
> The following assumes you have installed the CLI tool using one of the methods above and have
> `plexos2duckdb` in the `PATH` environment variable.

Verify installation is successful by checking the version:

```shell
plexos2duckdb --version
```

Run the help command to see available options:

```shell
plexos2duckdb --help
```

Convert a solution zip file to a duckdb database:

```shell
plexos2duckdb convert --input "Model-DayAhead-Solution.zip" --output "Model-DayAhead-Solution.duckdb"
```

If the output database already exists, re-run with `--force` to overwrite it:

```shell
plexos2duckdb convert --input "Model-DayAhead-Solution.zip" --output "Model-DayAhead-Solution.duckdb" --force
```

Inspect an existing database to view metadata, source file info, model name, and table inventory
with row counts:

```shell
plexos2duckdb inspect --input "Model-DayAhead-Solution.duckdb"
```

Generate shell completions to stdout with the `generate-shell-completions` subcommand:

```shell
plexos2duckdb generate-shell-completions bash > ~/.local/share/bash-completion/completions/plexos2duckdb
plexos2duckdb generate-shell-completions zsh > ~/.zfunc/_plexos2duckdb
```

You may use any
[duckdb compatible database viewer](https://duckdb.org/docs/stable/core_extensions/ui) to
interactively explore the data with SQL:

<img width="1728" height="775" alt="image" src="https://github.com/user-attachments/assets/ad829556-bef1-4982-b7b3-f7a62d225985" />

### Python usage

Import the `PLEXOS2DuckDB` class from the `plexos2duckdb` package, create a client instance with the
path to your solution zip file, and call the `convert()` method to generate the DuckDB database.

You can also use the same client as a context manager to interact with the database connection
directly.

```python
from plexos2duckdb import PLEXOS2DuckDB

client = PLEXOS2DuckDB("./Model DAY_AHEAD Solution.zip")
output_path = client.convert() # "./Model DAY_AHEAD Solution.duckdb"

with client as db:
    # assumes output_path exists at "./Model DAY_AHEAD Solution.duckdb"
    print(db.connection.query("SELECT * FROM information_schema.tables"))
```
