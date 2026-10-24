fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        // libduckdb-sys 1.20000.0 can put the registrar after DuckDB's archive.
        // GNU ld does not rescan earlier archives. Request this symbol up front;
        // the existing static constructor still performs extension registration.
        println!("cargo:rustc-link-arg=-Wl,--undefined=duckdb_register_static_extensions");
    }
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        println!("cargo:rustc-link-lib=Rstrtmgr");
    }
    // NOTE: This will output everything, and requires all features enabled.
    // NOTE: See the specific builder documentation for configuration options.
    let build = vergen_gitcl::Build::all_build();
    let cargo = vergen_gitcl::Cargo::all_cargo();
    let gitcl = vergen_gitcl::Gitcl::all_git();
    let rustc = vergen_gitcl::Rustc::all_rustc();
    let si = vergen_gitcl::Sysinfo::all_sysinfo();

    vergen_gitcl::Emitter::default()
        .default_on_error()
        .add_instructions(&build)?
        .add_instructions(&cargo)?
        .add_instructions(&gitcl)?
        .add_instructions(&rustc)?
        .add_instructions(&si)?
        .emit()?;
    Ok(())
}
