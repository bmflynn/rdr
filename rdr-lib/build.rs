use std::env::var_os;
use std::error::Error;
use std::fs::copy;
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    include_default_configs()?;
    Ok(())
}

fn etc_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("etc").join(name)
}

fn include_default_configs() -> Result<(), Box<dyn Error>> {
    for name in ["npp", "j01", "j02", "j03"] {
        let fname = format!("{name}.config.yaml");
        let src_path = etc_path(&fname);
        let dest_path = Path::new(&var_os("OUT_DIR").unwrap()).join(&fname);
        copy(&src_path, dest_path)?;
    }
    Ok(())
}
