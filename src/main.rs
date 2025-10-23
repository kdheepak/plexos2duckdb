use clap::Parser;
use color_eyre::{
    Result,
    eyre::{ContextCompat, eyre},
};
use plexos2duckdb;

#[derive(Parser)]
#[command(author, version = plexos2duckdb::utils::version(), about, long_about = None)]
struct Args {
    /// Path to the PLEXOS solution file or folder (either XML or ZIP containing XML, or solution folder)
    #[arg(short, long)]
    input: std::path::PathBuf,
    /// Path to the output DuckDB file (leave empty to use the same name as input)
    #[arg(short, long)]
    output: Option<std::path::PathBuf>,
    /// Print a summary of the dataset
    #[arg(long, default_value_t = false)]
    print_summary: bool,
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

fn resolve_output_path(input: &std::path::PathBuf, output: Option<std::path::PathBuf>) -> Result<std::path::PathBuf> {
    let output_path = if let Some(output_path) = output { output_path } else { input.with_extension("duckdb") };
    let output_path =
        if output_path.extension().is_none() { output_path.with_extension("duckdb") } else { output_path };
    if output_path.exists() {
        std::fs::remove_file(&output_path)?;
    }
    Ok(output_path)
}

fn run(args: Args) -> Result<()> {
    let input_path = resolve_input_path(&args.input)?;
    let input_dir = input_path.parent().ok_or_else(|| eyre!("Input path has no parent directory"))?;
    let output_path = resolve_output_path(&input_path, args.output)?;

    // Extract model name from the file name
    let file_name =
        input_path.file_name().context("File name must exist")?.to_str().context("File name must be valid UTF-8")?;
    let model_name = file_name
        .trim_start_matches("Model ")
        .trim_end_matches(" Solution") // if input_path is a folder
        .trim_end_matches(" Solution.zip") // if input_path is a zip file
        .trim_end_matches(" Solution.xml"); // if input path is a xml file

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
                .with_zip_file(&actual_input_path)?;
            // Look for a log file with the correct model name pattern
            let log_path = actual_input_path
                .parent()
                .ok_or_else(|| eyre!("Could not determine parent directory for input file"))?
                .join(format!("Model ( {} ) Log.txt", model_name));
            if log_path.exists() {
                let log = std::fs::read_to_string(&log_path)?;
                ds = ds.with_simulation_log(log);
            }
            ds
        } else if actual_input_path.extension().map_or(false, |ext| ext.eq_ignore_ascii_case("xml")) {
            let mut ds = plexos2duckdb::SolutionDataset::default()
                .with_model_name(model_name.to_string())
                .with_xml_file(&actual_input_path)?;
            let log_path = input_dir.join(format!("Model ( {} ) Log.txt", model_name));
            if log_path.exists() {
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
        dataset.with_run_stats(run_stats)
    } else {
        dataset
    };

    if args.print_summary {
        dataset.print_summary();
        return Ok(());
    }
    dataset.to_duckdb(&output_path)?;
    println!("DuckDB database created at: {}", output_path.display());
    Ok(())
}

fn main() -> Result<()> {
    color_eyre::install()?;
    let args = Args::parse();
    run(args)
}
