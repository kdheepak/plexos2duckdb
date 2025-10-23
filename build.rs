fn main() -> Result<(), Box<dyn std::error::Error>> {
    // NOTE: This will output everything, and requires all features enabled.
    // NOTE: See the specific builder documentation for configuration options.
    let build = vergen_gitcl::BuildBuilder::all_build()?;
    let cargo = vergen_gitcl::CargoBuilder::all_cargo()?;
    let gitcl = vergen_gitcl::GitclBuilder::default().all().describe(false, true, None).build()?;
    let rustc = vergen_gitcl::RustcBuilder::all_rustc()?;
    let si = vergen_gitcl::SysinfoBuilder::all_sysinfo()?;

    vergen_gitcl::Emitter::default()
        .add_instructions(&build)?
        .add_instructions(&cargo)?
        .add_instructions(&gitcl)?
        .add_instructions(&rustc)?
        .add_instructions(&si)?
        .emit()?;
    Ok(())
}
