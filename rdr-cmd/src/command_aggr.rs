use anyhow::{Context, Result};
use hdf5::File;
use rdr::{
    config::{get_default, Config, ProductSpec, SatSpec},
    write_rdr_granule, GranuleMeta, Meta, Rdr,
};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};
use tracing::{error, info, info_span, warn};

use crate::command_extract::extract;

struct Item {
    path: PathBuf,
    product: ProductSpec,
    sat: SatSpec,
    meta: GranuleMeta,
}

fn get_config(satid: &str) -> Result<Config> {
    get_default(satid)
        .expect("failed to get default config")
        .context("lookup failed")
}

pub fn create_file() -> Result<File> {
    let file = File::create("output.h5")?;
    file.create_group("/All_Data")?;
    file.create_group("/Data_Products")?;
    Ok(file)
}

pub fn aggreggate<O: AsRef<Path>>(inputs: &[PathBuf], workdir: O) -> Result<PathBuf> {
    let workdir = workdir.as_ref().to_path_buf();
    // short_name to RDRs
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

        let input_meta = Meta::from_file(input)?;
        let config = get_config(&input_meta.platform.to_lowercase())
            .with_context(|| format!("Failed to lookup spacecraft config for {input:?}"))?;
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
            let Some(meta) = input_meta
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
                    path: output.path.clone(),
                    meta: meta.clone(),
                    sat: config.satellite.clone(),
                    product: product.clone(),
                });
        }
    }
    info!(
        "extracted {} extracted granules from {} files",
        granule_count,
        inputs.len()
    );

    // All RDRs have been extracted, now shove them into the new file.
    let file = create_file()?;
    for (short_name, granules) in outputs.iter() {
        for (gran_idx, item) in granules.iter().enumerate() {
            let data = std::fs::read(&item.path)?;
            let rdr = Rdr::from_data(&item.sat, &item.product, &item.meta.begin, data)
                .with_context(|| format!("creating RDR {short_name} granule {gran_idx}"))?;
            write_rdr_granule(&file, gran_idx, &rdr)
                .with_context(|| format!("writing RDR {short_name} granule {gran_idx}"))?;
        }
    }

    Ok("output.h5".into())
}
