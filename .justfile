set shell := ["bash", "-euo", "pipefail", "-c"]

fixture_repo := "https://github.com/kdheepak/plexos-solution-files"
fixture_dir := "tests/data/plexos-solution-files"

download-test-data:
    if [[ ! -d {{fixture_dir}} ]]; then if ! command -v git >/dev/null; then echo "Error: git is required to download test fixtures into {{fixture_dir}}" >&2; exit 1; fi; git clone --depth 1 {{fixture_repo}} {{fixture_dir}}; fi

test: download-test-data
    cargo test
