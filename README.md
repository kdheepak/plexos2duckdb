# plexos2duckdb

This is a command line tool to convert PLEXOS output files to a DuckDB database.

## Installation

### Option 1: Download prebuilt binary

Prebuilt binaries are available from the
[GitHub Releases](https://github.com/epri-dev/plexos2duckdb/releases) page.

1. Visit the [Releases](https://github.com/epri-dev/plexos2duckdb/releases) page.
2. Download the appropriate binary for your operating system
3. Extract the archive:

   ```shell
   tar -xzf plexos2duckdb-<platform>.tar.gz
   ```

   or on Windows:

   ```powershell
   Expand-Archive plexos2duckdb-<platform>.zip -DestinationPath .
   ```

4. Copy the binary to a directory in your PATH:

   ```shell
   # MacOS/Linux
   cp plexos2duckdb ~/local/bin/
   # Windows
   copy plexos2duckdb.exe %USERPROFILE%\local\bin\
   ```

   Make sure `~/local/bin/` is in your PATH.

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

3. Copy the binary to a directory in your PATH:

   ```shell
   # MacOS/Linux
   cp target/release/plexos2duckdb ~/local/bin/
   # Windows
   copy target\release\plexos2duckdb.exe %USERPROFILE%\local\bin\
   ```

   Make sure `~/local/bin/` is in your PATH.

## Usage

You can verify installation is successful by checking the version:

```shell
plexos2duckdb --version
```

You can run the help command to see available options:

```shell
plexos2duckdb --help
```

For usage:

```shell
plexos2duckdb --input <input_file> --output <output_file>
```

For SQA testing

```shell
plexos2duckdb --input "Model-DayAhead-Solution.zip" --output "Model-DayAhead-Solution.duckdb"
```
