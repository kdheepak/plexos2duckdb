use std::sync::OnceLock;

pub const PLEXOS2DUCKDB_CLI_VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "-", env!("VERGEN_GIT_DESCRIBE"));
pub const PLEXOS2DUCKDB_CLI_BUILD_DATE: &str = env!("VERGEN_BUILD_DATE");
pub const PLEXOS2DUCKDB_CLI_TARGET_TRIPLE: &str = env!("VERGEN_CARGO_TARGET_TRIPLE");
pub const PLEXOS2DUCKDB_CLI_BUILD_FEATURES: &str = env!("VERGEN_CARGO_FEATURES");

fn build_info() -> String {
    format!("{PLEXOS2DUCKDB_CLI_VERSION} ({PLEXOS2DUCKDB_CLI_BUILD_DATE} {PLEXOS2DUCKDB_CLI_TARGET_TRIPLE})",)
}

static VERSION: OnceLock<String> = OnceLock::new();

pub fn version() -> &'static str {
    VERSION.get_or_init(build_info)
}
