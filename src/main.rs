use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use color_eyre::{
    Result,
    eyre::{ContextCompat, eyre},
};
use console::Term;
use ctrlc;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use owo_colors::OwoColorize;
use plexos2duckdb;
use tabled::{Table, Tabled, settings::Style};

#[derive(Parser)]
#[command(author, version = plexos2duckdb::utils::version(), about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Convert a PLEXOS solution file to DuckDB
    Convert(ConvertArgs),
    /// Show operational metadata from a generated DuckDB database
    Inspect(InspectArgs),
}

#[derive(Parser, Debug)]
struct ConvertArgs {
    /// Path to the PLEXOS solution file or folder (either XML or ZIP containing XML, or solution folder)
    #[arg(short, long)]
    input: std::path::PathBuf,
    /// Path to the output DuckDB file (leave empty to use the same name as input)
    #[arg(short, long)]
    output: Option<std::path::PathBuf>,
    /// Overwrite the output DuckDB file if it already exists
    #[arg(long, default_value_t = false)]
    force: bool,
    /// Print a summary of the dataset
    #[arg(long, default_value_t = false)]
    print_summary: bool,
    /// Disable progress bar output
    #[arg(long, default_value_t = false)]
    no_progress_bar: bool,
    /// Stage in memory then copy to disk (faster, but uses more RAM)
    #[arg(long, default_value_t = false)]
    in_memory: bool,
    /// Number of threads to use when writing time series data tables
    #[arg(long)]
    n_threads: Option<std::num::NonZeroUsize>,
}

#[derive(Parser, Debug)]
struct InspectArgs {
    /// Path to a generated DuckDB database
    #[arg(short, long)]
    input: std::path::PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DatabaseMetadata {
    database: String,
    converter_version: String,
    source_file: String,
    model_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Tabled)]
struct MetadataRow {
    field: String,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Tabled)]
struct TableInventoryRow {
    schema: String,
    table: String,
    kind: String,
    row_count: String,
}

fn resolve_input_path(input: &std::path::Path) -> Result<std::path::PathBuf> {
    let path = if input.is_file() {
        let ext = input.extension().and_then(|e| e.to_str()).map(|e| e.to_ascii_lowercase());

        match ext.as_deref() {
            Some("zip") | Some("xml") => input.to_path_buf(),
            _ => return Err(eyre!("Input file must have .zip or .xml extension")),
        }
    } else if input.is_dir() {
        let mut zip_files = std::fs::read_dir(input)?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |ext| ext.eq_ignore_ascii_case("zip")))
            .collect::<Vec<_>>();

        if zip_files.len() == 1 {
            zip_files.remove(0)
        } else if zip_files.is_empty() {
            return Err(eyre!("No .zip files found in directory"));
        } else {
            return Err(eyre!("Multiple .zip files found in directory"));
        }
    } else {
        return Err(eyre!("Path is neither a file nor a directory: {}", input.display()));
    };
    if !path.exists() {
        return Err(eyre!("File or directory does not exist: {}", input.display()));
    }
    Ok(path)
}

fn resolve_output_path(
    input: &std::path::Path,
    output: Option<std::path::PathBuf>,
    force: bool,
) -> Result<std::path::PathBuf> {
    let output_path = if let Some(output_path) = output { output_path } else { input.with_extension("duckdb") };
    let output_path =
        if output_path.extension().is_none() { output_path.with_extension("duckdb") } else { output_path };
    if output_path.exists() {
        if !force {
            return Err(eyre!(
                "Output file already exists: \"{}\". Re-run with `--force` to overwrite it",
                output_path.display().to_string().bold()
            ));
        }
        std::fs::remove_file(&output_path)?;
    }
    Ok(output_path)
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}


fn metadata_value(metadata: &std::collections::HashMap<String, String>, key: &str) -> Result<String> {
    metadata.get(key).cloned().ok_or_else(|| eyre!("Missing metadata key in main.plexos2duckdb: {key}"))
}

fn load_database_metadata(con: &duckdb::Connection) -> Result<DatabaseMetadata> {
    let mut stmt = con.prepare("SELECT key, value FROM main.plexos2duckdb")?;
    let rows = stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?.unwrap_or_default())))?;

    let mut metadata = std::collections::HashMap::new();
    for row in rows {
        let (key, value) = row?;
        metadata.insert(key, value);
    }

    Ok(DatabaseMetadata {
        database: String::new(),
        converter_version: metadata_value(&metadata, "plexos2duckdb_version")?,
        source_file: metadata_value(&metadata, "plexos_file")?,
        model_name: metadata_value(&metadata, "model_name")?,
    })
}

fn load_table_inventory(con: &duckdb::Connection) -> Result<Vec<TableInventoryRow>> {
    let mut stmt = con.prepare(
        "
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
        ",
    )?;
    let tables = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;

    let mut inventory = Vec::new();
    for table in tables {
        let (schema, table_name, table_type) = table?;
        let kind = if table_type == "BASE TABLE" { "table" } else { "view" }.to_string();
        let row_count = if table_type == "BASE TABLE" {
            let sql = format!(
                "SELECT COUNT(*) FROM {}.{}",
                quote_ident(&schema),
                quote_ident(&table_name)
            );
            let count: i64 = con.query_row(&sql, [], |row| row.get(0))?;
            count.to_string()
        } else {
            "-".to_string()
        };
        inventory.push(TableInventoryRow { schema, table: table_name, kind, row_count });
    }

    Ok(inventory)
}

fn inspect_database(args: InspectArgs) -> Result<()> {
    if !args.input.exists() {
        return Err(eyre!("DuckDB file does not exist: {}", args.input.display()));
    }
    let con = duckdb::Connection::open(&args.input)?;
    let mut metadata = load_database_metadata(&con)?;
    metadata.database = args.input.display().to_string();
    let inventory = load_table_inventory(&con)?;

    let metadata_rows = vec![
        MetadataRow { field: "database".to_string(), value: metadata.database },
        MetadataRow { field: "converter version".to_string(), value: metadata.converter_version },
        MetadataRow { field: "source file".to_string(), value: metadata.source_file },
        MetadataRow { field: "model name".to_string(), value: metadata.model_name },
    ];

    println!("Metadata");
    println!("{}", Table::new(metadata_rows).with(Style::rounded()));
    println!();
    println!("Inventory");
    println!("{}", Table::new(inventory).with(Style::rounded()));
    Ok(())
}

fn convert(args: ConvertArgs) -> Result<()> {
    let input_path = resolve_input_path(&args.input)?;
    let input_dir = input_path.parent().ok_or_else(|| eyre!("Input path has no parent directory"))?;
    let output_path = resolve_output_path(&input_path, args.output, args.force)?;

    let mut mp = None;
    let mut pb = None;
    let mut data_tables_pb = None;
    let mut data_merge_pb = None;
    let mut worker_tables_pb: std::collections::BTreeMap<usize, ProgressBar> = std::collections::BTreeMap::new();
    let mut current_table = None;
    let mut last_data_table_was_final = false;
    let mut last_msg = String::new();
    let mut start_time = None;
    let mut last_mark = None;
    let mut total_line = None;
    let mut term = None;
    if !args.no_progress_bar {
        let term_handle = Term::stderr();
        let _ = term_handle.hide_cursor();
        let multi = MultiProgress::new();
        multi.set_draw_target(ProgressDrawTarget::term(term_handle.clone(), 120));
        let spinner = multi.add(ProgressBar::new_spinner());
        spinner.set_style(ProgressStyle::with_template("{spinner:.green} {elapsed_precise:.dim} {msg}").unwrap());
        spinner.enable_steady_tick(Duration::from_millis(120));
        start_time = Some(Instant::now());
        last_mark = start_time;
        pb = Some(spinner);
        mp = Some(multi);
        term = Some(term_handle);
    }
    if let Some(term) = term.clone() {
        let _ = ctrlc::set_handler(move || {
            let _ = term.show_cursor();
            eprintln!();
            std::process::exit(130);
        });
    }
    struct CursorGuard(Option<Term>);
    impl Drop for CursorGuard {
        fn drop(&mut self) {
            if let Some(term) = self.0.as_ref() {
                let _ = term.show_cursor();
            }
        }
    }
    let _cursor_guard = CursorGuard(term.clone());
    let mut report = |msg: &str| {
        if let Some(spinner) = pb.as_ref() {
            if msg != last_msg {
                let now = Instant::now();
                if !last_msg.is_empty() {
                    let delta = last_mark.map(|s| now.duration_since(s)).unwrap_or_default();
                    let line = format!("[+{:>6.2}s]", delta.as_secs_f64()).dimmed().to_string();
                    let msg = last_msg.cyan().to_string();
                    spinner.println(format!("{line} {msg}"));
                }
                spinner.set_message(msg.to_string());
                last_msg.clear();
                last_msg.push_str(msg);
                last_mark = Some(now);
                if let Some(start) = start_time {
                    let total = now.duration_since(start);
                    total_line = Some(format!("Total time: {:.2}s", total.as_secs_f64()));
                }
            }
        }
    };

    let mut report_data = |event: plexos2duckdb::ProgressEvent| {
        if args.no_progress_bar {
            return;
        }
        match event {
            plexos2duckdb::ProgressEvent::DataTableStart { index, total, table_name, keys } => {
                if keys == 0 {
                    return;
                }
                last_data_table_was_final = index == total;
                current_table = Some(table_name);
                if data_tables_pb.is_none() {
                    if let Some(multi) = mp.as_ref() {
                        let bar = multi.add(ProgressBar::new(total as u64));
                        bar.set_style(
                            ProgressStyle::with_template(
                                "{prefix:>9.bold} {bar:28.cyan/blue} {pos:>2}/{len:2} {elapsed_precise:.dim} {msg:.cyan}",
                            )
                            .unwrap(),
                        );
                        bar.set_prefix("tables");
                        data_tables_pb = Some(bar);
                    }
                }
                if let Some(bar) = data_tables_pb.as_ref() {
                    bar.set_length(total as u64);
                    bar.set_position(index as u64);
                    let table = current_table.as_deref().unwrap_or("data");
                    bar.set_message(format!("{table} ({keys} keys)"));
                }
            },
            plexos2duckdb::ProgressEvent::DataTableEnd => {
                current_table = None;
                if last_data_table_was_final {
                    if let Some(bar) = data_tables_pb.as_ref() {
                        bar.set_message("done");
                    }
                }
            },
            plexos2duckdb::ProgressEvent::DataWorkerTableStart { worker_id, index, total, table_name, keys } => {
                if !worker_tables_pb.contains_key(&worker_id) {
                    if let Some(multi) = mp.as_ref() {
                        let bar = multi.add(ProgressBar::new(total as u64));
                        bar.set_style(
                            ProgressStyle::with_template(
                                "{prefix:>9.bold} {bar:28.green/blue} {pos:>2}/{len:2} {elapsed_precise:.dim} {msg:.green}",
                            )
                            .unwrap(),
                        );
                        bar.set_prefix(format!("thread-{}", worker_id + 1));
                        worker_tables_pb.insert(worker_id, bar);
                    }
                }
                if let Some(bar) = worker_tables_pb.get(&worker_id) {
                    bar.set_length(total as u64);
                    bar.set_position(index.saturating_sub(1) as u64);
                    bar.set_message(format!("{table_name} ({keys} keys)"));
                }
            },
            plexos2duckdb::ProgressEvent::DataWorkerTableEnd { worker_id, index, total } => {
                if let Some(bar) = worker_tables_pb.get(&worker_id) {
                    bar.set_length(total as u64);
                    bar.set_position(index as u64);
                    if index == total {
                        bar.set_message("done");
                    }
                }
            },
            plexos2duckdb::ProgressEvent::DataMergeTableStart { index, total, table_name } => {
                if data_merge_pb.is_none() {
                    if let Some(multi) = mp.as_ref() {
                        let bar = multi.add(ProgressBar::new(total as u64));
                        bar.set_style(
                            ProgressStyle::with_template(
                                "{prefix:>9.bold} {bar:28.yellow/blue} {pos:>3}/{len:3} {elapsed_precise:.dim} {msg:.yellow}",
                            )
                            .unwrap(),
                        );
                        bar.set_prefix("merge");
                        data_merge_pb = Some(bar);
                    }
                }
                if let Some(bar) = data_merge_pb.as_ref() {
                    bar.set_length(total as u64);
                    bar.set_position(index.saturating_sub(1) as u64);
                    bar.set_message(table_name);
                }
            },
            plexos2duckdb::ProgressEvent::DataMergeTableEnd { index, total } => {
                if let Some(bar) = data_merge_pb.as_ref() {
                    bar.set_length(total as u64);
                    bar.set_position(index as u64);
                    if index == total {
                        bar.set_message("done");
                    }
                }
            },
        }
    };

    let file_name =
        input_path.file_name().context("File name must exist")?.to_str().context("File name must be valid UTF-8")?;
    let model_name = file_name
        .trim_start_matches("Model ")
        .trim_end_matches(" Solution")
        .trim_end_matches(" Solution.zip")
        .trim_end_matches(" Solution.xml");

    let dataset = {
        let actual_input_path = if input_path.is_dir() {
            let mut zip_files = std::fs::read_dir(&input_path)?
                .filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| p.extension().map_or(false, |ext| ext.eq_ignore_ascii_case("zip")))
                .collect::<Vec<_>>();
            if zip_files.len() == 1 {
                zip_files.remove(0)
            } else if zip_files.is_empty() {
                return Err(eyre!("No .zip files found in directory"));
            } else {
                return Err(eyre!("Multiple .zip files found in directory"));
            }
        } else {
            input_path.clone()
        };
        if actual_input_path.extension().map_or(false, |ext| ext.eq_ignore_ascii_case("zip")) {
            let mut ds = plexos2duckdb::SolutionDataset::default()
                .with_model_name(model_name.to_string())
                .with_zip_file_with_progress(&actual_input_path, &mut report)?;
            let log_path = actual_input_path
                .parent()
                .ok_or_else(|| eyre!("Could not determine parent directory for input file"))?
                .join(format!("Model ( {} ) Log.txt", model_name));
            if log_path.exists() {
                report("Reading simulation log");
                let log = std::fs::read_to_string(&log_path)?;
                ds = ds.with_simulation_log(log);
            }
            ds
        } else if actual_input_path.extension().map_or(false, |ext| ext.eq_ignore_ascii_case("xml")) {
            let mut ds = plexos2duckdb::SolutionDataset::default()
                .with_model_name(model_name.to_string())
                .with_xml_file_with_progress(&actual_input_path, &mut report)?;
            let log_path = input_dir.join(format!("Model ( {} ) Log.txt", model_name));
            if log_path.exists() {
                report("Reading simulation log");
                let log = std::fs::read_to_string(&log_path)?;
                ds = ds.with_simulation_log(log);
            }
            ds
        } else {
            return Err(eyre!("Input file must have .zip or .xml extension"));
        }
    };

    let run_stats = input_dir.join(std::path::Path::new("runstats.json"));
    let dataset = if let Ok(run_stats) = std::fs::read_to_string(&run_stats) {
        report("Reading run stats");
        dataset.with_run_stats(run_stats)
    } else {
        dataset
    };

    if args.print_summary {
        if let Some(spinner) = pb.as_ref() {
            if !last_msg.is_empty() {
                let now = Instant::now();
                let delta = last_mark.map(|s| now.duration_since(s)).unwrap_or_default();
                let line = format!("[+{:>6.2}s]", delta.as_secs_f64()).dimmed().to_string();
                let msg = last_msg.cyan().to_string();
                spinner.println(format!("{line} {msg}"));
            }
            spinner.finish_and_clear();
        }
        if let Some(term) = term.as_ref() {
            let _ = term.show_cursor();
        }
        if let Some(bar) = data_tables_pb.as_ref() {
            bar.finish_and_clear();
        }
        if let Some(bar) = data_merge_pb.as_ref() {
            bar.finish_and_clear();
        }
        for bar in worker_tables_pb.values() {
            bar.finish_and_clear();
        }
        if let Some(line) = total_line.as_ref() {
            eprintln!("{}", line.green());
        }
        dataset.print_summary();
        return Ok(());
    }
    report("Creating DuckDB database");
    let mode =
        if args.in_memory { plexos2duckdb::DbWriteMode::InMemoryThenCopy } else { plexos2duckdb::DbWriteMode::Direct };
    let mut builder = dataset.to_duckdb(&output_path).with_mode(mode);
    if let Some(threads) = args.n_threads {
        builder = builder.with_data_write_threads(threads.get());
    }
    let builder =
        if !args.no_progress_bar { builder.with_progress(&mut report).with_events(&mut report_data) } else { builder };
    builder.run()?;
    if let Some(spinner) = pb.as_ref() {
        if !last_msg.is_empty() {
            let now = Instant::now();
            let delta = last_mark.map(|s| now.duration_since(s)).unwrap_or_default();
            let line = format!("[+{:>6.2}s]", delta.as_secs_f64()).dimmed().to_string();
            let msg = last_msg.cyan().to_string();
            spinner.println(format!("{line} {msg}"));
        }
        spinner.finish_and_clear();
    }
    if let Some(term) = term.as_ref() {
        let _ = term.show_cursor();
    }
    if let Some(bar) = data_tables_pb.as_ref() {
        bar.finish_and_clear();
    }
    if let Some(bar) = data_merge_pb.as_ref() {
        bar.finish_and_clear();
    }
    for bar in worker_tables_pb.values() {
        bar.finish_and_clear();
    }
    if let Some(line) = total_line.as_ref() {
        eprintln!("{}", line.green());
    }
    println!("{} {}", "DuckDB database created at:".green(), output_path.display().to_string().blue());
    Ok(())
}

fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Convert(args) => convert(args),
        Command::Inspect(args) => inspect_database(args),
    }
}

fn main() {
    let cli = Cli::parse();
    if let Err(err) = run(cli) {
        eprintln!("{} {}", "Error:".red().bold(), err);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    #[test]
    fn resolve_output_path_rejects_existing_file_without_force() -> Result<()> {
        let temp_dir = tempfile::TempDir::new()?;
        let input_path = temp_dir.path().join("input.zip");
        std::fs::File::create(&input_path)?;
        let output_path = temp_dir.path().join("existing.duckdb");
        std::fs::File::create(&output_path)?;

        let err = resolve_output_path(&input_path, Some(output_path.clone()), false).unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "Output file already exists: \"{}\". Re-run with `--force` to overwrite it",
                output_path.display().to_string().bold()
            )
        );
        assert!(output_path.exists(), "existing output should be left untouched without --force");
        Ok(())
    }

    #[test]
    fn resolve_output_path_deletes_existing_file_with_force() -> Result<()> {
        let temp_dir = tempfile::TempDir::new()?;
        let input_path = temp_dir.path().join("input.zip");
        std::fs::File::create(&input_path)?;
        let output_path = temp_dir.path().join("existing.duckdb");
        let mut output_file = std::fs::File::create(&output_path)?;
        output_file.write_all(b"sentinel")?;

        let resolved = resolve_output_path(&input_path, Some(output_path.clone()), true)?;
        assert_eq!(resolved, output_path);
        assert!(!output_path.exists(), "--force should delete any existing output file before conversion continues");
        Ok(())
    }

    #[test]
    fn load_database_metadata_reads_required_fields() -> Result<()> {
        let con = duckdb::Connection::open_in_memory()?;
        con.execute_batch(
            "
            CREATE TABLE main.plexos2duckdb (key TEXT, value TEXT);
            INSERT INTO main.plexos2duckdb VALUES
              ('plexos2duckdb_version', '0.1.2-test'),
              ('plexos_file', '/tmp/input.zip'),
              ('model_name', 'Model A');
            ",
        )?;

        let metadata = load_database_metadata(&con)?;
        assert_eq!(
            metadata,
            DatabaseMetadata {
                database: String::new(),
                converter_version: "0.1.2-test".to_string(),
                source_file: "/tmp/input.zip".to_string(),
                model_name: "Model A".to_string(),
            }
        );
        Ok(())
    }

    #[test]
    fn load_table_inventory_reports_tables_and_views() -> Result<()> {
        let con = duckdb::Connection::open_in_memory()?;
        con.execute_batch(
            "
            CREATE SCHEMA raw;
            CREATE SCHEMA report;
            CREATE TABLE raw.items(id INTEGER);
            INSERT INTO raw.items VALUES (1), (2), (3);
            CREATE VIEW report.items_view AS SELECT * FROM raw.items;
            ",
        )?;

        let inventory = load_table_inventory(&con)?;
        assert!(inventory.iter().any(|row| row.schema == "raw" && row.table == "items" && row.kind == "table" && row.row_count == "3"));
        assert!(inventory.iter().any(|row| row.schema == "report" && row.table == "items_view" && row.kind == "view" && row.row_count == "-"));
        Ok(())
    }

    #[test]
    fn inspect_tables_render_with_tabled() {
        let metadata_rows = vec![
            MetadataRow { field: "database".to_string(), value: "/tmp/model.duckdb".to_string() },
            MetadataRow { field: "model name".to_string(), value: "Model A".to_string() },
        ];
        let inventory = vec![TableInventoryRow {
            schema: "raw".to_string(),
            table: "items".to_string(),
            kind: "table".to_string(),
            row_count: "3".to_string(),
        }];

        let metadata_table = Table::new(metadata_rows).with(Style::rounded()).to_string();
        let inventory_table = Table::new(inventory).with(Style::rounded()).to_string();

        assert!(metadata_table.contains("field"));
        assert!(metadata_table.contains("database"));
        assert!(metadata_table.contains("/tmp/model.duckdb"));
        assert!(inventory_table.contains("schema"));
        assert!(inventory_table.contains("row_count"));
        assert!(inventory_table.contains("items"));
        assert!(inventory_table.contains("3"));
    }

    #[test]
    fn user_facing_error_message_is_concise() -> Result<()> {
        let temp_dir = tempfile::TempDir::new()?;
        let input_path = temp_dir.path().join("input.zip");
        let output_path = temp_dir.path().join("existing.duckdb");
        std::fs::File::create(&input_path)?;
        std::fs::File::create(&output_path)?;

        let err = resolve_output_path(&input_path, Some(output_path.clone()), false).unwrap_err();
        let rendered = format!("{} {}", "Error:".red().bold(), err);

        assert!(rendered.contains(&format!(
            "Output file already exists: \"{}\". Re-run with `--force` to overwrite it",
            output_path.display().to_string().bold()
        )));
        assert!(rendered.contains("Error:"));
        assert!(rendered.contains("\u{1b}["), "expected ANSI color escape sequence in rendered prefix");
        assert!(!rendered.contains("Location:"));
        assert!(!rendered.contains("Backtrace omitted"));
        Ok(())
    }
}
