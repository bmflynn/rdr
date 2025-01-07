use anyhow::{Context, Result};
use hdf5::File;
use ndarray::arr1;
use rdr::{
    config::{get_default, Config},
    GranuleMeta, Meta, Rdr, Time,
};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::OnceLock,
};
use tracing::{error, info, info_span, warn};

use crate::command_extract::extract;

struct Item {
    meta: GranuleMeta,
    path: PathBuf,
    short_name: String,
    product_id: String,
}

fn get_sat(name: &str) -> Result<&str> {
    name.split("_")
        .nth(1)
        .with_context(|| format!("failed to determine satellite from {name}"))
}

fn get_config(satid: &str) -> Result<Config> {
    get_default(satid)
        .expect("failed to get default config")
        .context("lookup failed")
}

pub fn create_file() -> Result<File> {
    let file = File::create("output.h5")?;
    file.group("/All_Data")?;
    file.group("/Data_Products")?;
    Ok(file)
}

pub fn aggreggate<O: AsRef<Path>>(inputs: &[PathBuf], workdir: O) -> Result<PathBuf> {
    let workdir = workdir.as_ref().to_path_buf();
    let mut outputs: HashMap<String, Vec<Item>> = Default::default();
    let mut granule_count: usize = 0;

    // Extract RDR data to workdir in dirs named for input file names. Collect data necessary to
    // construct aggregated file.
    for input in inputs {
        let name = input.file_name().expect("should have file name");

        let span = info_span!("rdr_input", ?name);
        let _guard = span.enter();

        // Extract RDR granules
        let extracted_outputs = match extract(input, &workdir, None, None) {
            Ok(arr) => arr,
            Err(err) => {
                error!("failed to extract granules from {input:?}; skipping: {err}");
                continue;
            }
        };

        // Initialize global OnceLocks. First file sets both config and what will become the global
        // file metadata.
        let input_meta = Meta::from_file(input)?;

        let config =
            get_config(&input_meta.platform.to_lowercase()).expect("no config for platform");
        for output in &extracted_outputs {
            granule_count += 1;
            info!("extracted {}/{}", output.short_name, output.granule_id);
            let Some(product) = config
                .products
                .iter()
                .find(|p| p.short_name == output.short_name)
            else {
                warn!("no product for short_name {}; skipping", output.short_name);
                continue;
            };
            let Some(granule) = input_meta
                .granules
                .get(&product.short_name.clone())
                .unwrap()
                .iter()
                .find(|g| g.id == output.granule_id)
            else {
                warn!(
                    "no granule in metadata matching granule id {}; skipping",
                    output.granule_id
                );
                continue;
            };
            outputs
                .entry(output.short_name.clone())
                .or_default()
                .push(Item {
                    meta: granule.clone(),
                    path: output.path.clone(),
                    short_name: output.short_name.clone(),
                    product_id: product.product_id.clone(),
                });
        }
    }

    info!(
        "extracted {} extracted granules from {} files",
        granule_count,
        inputs.len()
    );

    let file = create_file()?;
    for (short_name, granules) in outputs.iter() {
        for (idx, granule) in granules.iter().enumerate() {
            let group_name = format!("/All_Data/{short_name}");
            if let Err(err) = file.group(&group_name) {
                error!("failed to open {group_name}; skipping: {err}");
                continue;
            }
            let data = std::fs::read(&granule.path)?;
            let name = format!("/All_Data/{short_name}/RawApplicationPackets_{idx}");
            file.new_dataset_builder()
                .with_data(&arr1(&data[..]))
                .create(name.clone().as_str())?;
        }
    }

    Ok("output.h5".into())
}
