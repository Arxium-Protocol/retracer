//! Exposes the Arxium git rev this build's wire types are pinned to (the
//! `rev = "…"` on the `xc-*` deps in the workspace `Cargo.toml`) as
//! `ARXIUM_NODE_REV`, so `--version` and `/v1/chains` can say which node
//! this Retracer follows without a constant that drifts from the pin.

use std::{env, fs, path::Path};

fn main() {
    let manifest = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap()).join("../../Cargo.toml");
    println!("cargo:rerun-if-changed={}", manifest.display());
    let text = fs::read_to_string(&manifest).expect("workspace Cargo.toml");
    let rev = text
        .lines()
        .filter(|l| l.contains("Arxium-Protocol/arxium"))
        .find_map(|l| l.split("rev = \"").nth(1)?.split('"').next())
        .expect("workspace Cargo.toml pins an Arxium-Protocol/arxium dep with rev = \"…\"");
    println!("cargo:rustc-env=ARXIUM_NODE_REV={rev}");
}
