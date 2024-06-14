use std::env::var_os;
use std::error::Error;
use std::fs::copy;
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    include_default_configs()?;
    include_leapsec_list()?;
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

/// Include our default leap-seconds.list file and bail if the file is too old.
fn include_leapsec_list() -> Result<(), Box<dyn Error>> {
    // const LEAPSEC_MAX_AGE_SECS: u64 = 86400 * 30;
    // const LEAPSEC_DOWNLOAD_URL: &str =
    //     "https://hpiers.obspm.fr/iers/bul/bulc/ntp/leap-seconds.list";
    //
    // let src_path = etc_path("leap-seconds.list");
    // if src_path.metadata()?.modified()?.elapsed()?.as_secs() > LEAPSEC_MAX_AGE_SECS {
    //     // let msg = format!("{source:?} needs to be updated");
    //     return Err(format!(
    //         "{src_path:?} needs to be updated. Download new one from {LEAPSEC_DOWNLOAD_URL}"
    //     )
    //     .into());
    // }

    let src_path = etc_path("leap-seconds.list");
    // copy the leap-seconds.list from project root to out dir where it it will be
    // included from in src/time.rs
    let dest_path = Path::new(&var_os("OUT_DIR").unwrap()).join("leap-seconds.list");
    copy(src_path, dest_path)?;

    Ok(())
}
