# plexos2duckdb

[![Release](https://img.shields.io/github/v/release/epri-dev/plexos2duckdb)](https://github.com/epri-dev/plexos2duckdb/releases)
[![License](https://img.shields.io/github/license/epri-dev/plexos2duckdb)](https://github.com/epri-dev/plexos2duckdb/blob/main/LICENSE)
[![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20MacOS%20%7C%20Linux-blue)]()
[![Downloads](https://img.shields.io/github/downloads/epri-dev/plexos2duckdb/total?color=brightgreen)](https://github.com/epri-dev/plexos2duckdb/releases)

This is a command line tool to convert PLEXOS solution files to a DuckDB database.

## Installation

### Option 1: Download prebuilt binary

Prebuilt binaries are available from the
[GitHub Releases](https://github.com/epri-dev/plexos2duckdb/releases) page.

1. Go to the [latest release](https://github.com/epri-dev/plexos2duckdb/releases/latest) page on GitHub.
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

### Option 2: Build from Source

If you prefer to build from source, ensure you have [Rust](https://www.rust-lang.org/tools/install)
and [Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html) installed.

1. Clone the repository:

   ```shell
   git clone https://github.com/epri-dev/plexos2duckdb.git
   cd plexos2duckdb
   ```

2. Build the binary:

   ```shell
   cargo build --release
   ```

   The compiled binary will be located at:

   ```
   target/release/plexos2duckdb
   ```

3. Copy the binary to a directory in your `PATH`:

   ```shell
   # MacOS/Linux
   cp target/release/plexos2duckdb ~/local/bin/
   # Windows
   copy target\release\plexos2duckdb.exe %USERPROFILE%\local\bin\
   ```

   You will have to make sure `~/local/bin/` is in your `PATH`.

## Quickstart

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
plexos2duckdb --input "Model-DayAhead-Solution.zip" --output "Model-DayAhead-Solution.duckdb"
```

Use any [duckdb compatible database viewer](https://duckdb.org/docs/stable/core_extensions/ui) to interactively explore the data with SQL:

<img width="1728" height="775" alt="image" src="https://github.com/user-attachments/assets/ad829556-bef1-4982-b7b3-f7a62d225985" />
