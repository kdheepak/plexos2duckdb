use std::sync::OnceLock;

pub const PLEXOS2DUCKDB_CLI_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const PLEXOS2DUCKDB_CLI_BUILD_DATE: &str = env!("VERGEN_BUILD_DATE");
pub const PLEXOS2DUCKDB_CLI_TARGET_TRIPLE: &str = env!("VERGEN_CARGO_TARGET_TRIPLE");
pub const PLEXOS2DUCKDB_CLI_BUILD_FEATURES: &str = env!("VERGEN_CARGO_FEATURES");

fn cli_version(git_describe: Option<&str>) -> String {
    match git_describe {
        Some(git_describe) => format!("{PLEXOS2DUCKDB_CLI_VERSION}-{git_describe}"),
        None => PLEXOS2DUCKDB_CLI_VERSION.to_owned(),
    }
}

fn build_info() -> String {
    let version = cli_version(option_env!("VERGEN_GIT_DESCRIBE"));
    format!("{version} ({PLEXOS2DUCKDB_CLI_BUILD_DATE} {PLEXOS2DUCKDB_CLI_TARGET_TRIPLE})",)
}

static VERSION: OnceLock<String> = OnceLock::new();

pub fn version() -> &'static str {
    VERSION.get_or_init(build_info)
}
