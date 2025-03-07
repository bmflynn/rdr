use anyhow::{bail, Context, Result};
use hdf5::File;
use rdr::{
    config::{get_default, Config, ProductSpec},
    write_rdr_granule, GranuleMeta, Meta, Rdr, Time,
};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};
use tracing::{error, info, info_span, warn};

use crate::command_extract::extract;

struct Item {
    path: PathBuf,
    product: ProductSpec,
    meta: GranuleMeta,
}

pub fn create_file(
    config: &Config,
    start: &Time,
    end: &Time,
    product_ids: &[String],
    workdir: &Path,
) -> Result<(PathBuf, File)> {
    let mut product_ids = Vec::from_iter(product_ids.iter().cloned());
    product_ids.sort();
    let created = Time::now();
    let fname = rdr::filename(
        &config.satellite.id,
        &config.origin,
        &config.mode,
        &created,
        start,
        end,
        &product_ids,
    );
    let fpath = workdir.join(&fname);
    let file = File::create(&fpath)?;

    rdr::write_rdr_meta(
        &file,
        &config.distributor,
        &config.satellite.mission,
        &config.satellite.short_name,
        &config.distributor,
        &created,
    )?;

    file.create_group("/All_Data")?;
    file.create_group("/Data_Products")?;
    Ok((fpath, file))
}

pub fn aggreggate<O: AsRef<Path>>(inputs: &[PathBuf], workdir: O) -> Result<PathBuf> {
    assert!(!inputs.is_empty());

    let workdir = workdir.as_ref().to_path_buf();
    // short_name to RDRs
    let mut outputs: HashMap<String, Vec<Item>> = Default::default();
    let mut granule_count: usize = 0;
    let mut start = Time::now();
    let mut end = Time::from_iet(0);
    let mut product_ids: HashSet<String> = HashSet::default();
    let mut config: Option<Config> = None;

    // Extract RDR data to workdir in dirs named for input file names. Collect data necessary to
    // construct aggregated file in next step.
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

        let mut input_meta = Meta::from_file(input)?;
        let input_satid = input_meta.platform.to_lowercase().clone();

        // Get config for the satellite indicated by the input, otherwise bail
        if config.is_none() {
            config = match get_default(&input_satid) {
                Some(c) => Some(c),
                None => bail!("No satelite configuration for {input_satid}"),
            };
        }
        let config = config.as_ref().expect("we set config above");
        // Make sure input satellites match
        if config.satellite.id != input_satid {
            bail!(
                "Cannot aggregate multiple satellites: {} != {}",
                config.satellite.id,
                input_satid
            );
        }

        for output in &extracted_outputs {
            granule_count += 1;

            // lookup product spec for this rdr in config
            info!("extracted {}/{}", output.short_name, output.granule_id);
            let Some(product) = config
                .products
                .iter()
                .find(|p| p.short_name == output.short_name)
            else {
                warn!("no product for short_name {}; skipping", output.short_name);
                continue;
            };

            // find the granule metadata for this rdr
            let Some(meta) = input_meta
                .granules
                .entry(product.short_name.clone())
                .or_default()
                .iter()
                .find(|g| g.id == output.granule_id)
            else {
                warn!(
                    "no granule in metadata matching granule id {}; skipping",
                    output.granule_id
                );
                continue;
            };

            // record the data we'll need later to write new file
            outputs
                .entry(output.short_name.clone())
                .or_default()
                .push(Item {
                    path: output.path.clone(),
                    meta: meta.clone(),
                    product: product.clone(),
                });

            if meta.collection.contains("SCIENCE") {
                start = Time::from_iet(std::cmp::min(start.iet(), meta.begin_time_iet));
                end = Time::from_iet(std::cmp::max(end.iet(), meta.end_time_iet));
            }
            product_ids.insert(product.product_id.to_string());
        }
    }
    if granule_count == 0 {
        bail!("No RDRs extracted");
    }

    info!(
        "extracted {} extracted granules from {} files",
        granule_count,
        inputs.len()
    );

    // Create new file from previously extracted rdrs
    let (fpath, file) = create_file(
        &config.expect("config should have been determined by inputs"),
        &start,
        &end,
        &Vec::from_iter(product_ids),
        &workdir,
    )?;
    info!("created {fpath:?}");

    // For each of our extracted RDRs, write it to the file we created
    for (short_name, granules) in outputs.iter_mut() {
        // granules must be sorted by time
        granules.sort_unstable_by_key(|item| item.meta.begin_time_iet);
        for (gran_idx, item) in granules.iter().enumerate() {
            let data = std::fs::read(&item.path)?;
            let rdr = Rdr {
                product_id: item.product.product_id.to_string(),
                meta: item.meta.clone(),
                data,
            };
            write_rdr_granule(&file, gran_idx, &rdr)
                .with_context(|| format!("writing RDR {short_name} granule {gran_idx}"))?;
        }
    }
    file.close().context("closing h5 file")?;

    let fname = fpath.file_name().context("getting file name")?;
    let mut fdest =
        std::fs::File::create(fname).with_context(|| format!("creating dest {fname:?}"))?;
    let mut fsrc =
        std::fs::File::open(&fpath).with_context(|| format!("opening aggr file {fpath:?}"))?;
    std::io::copy(&mut fsrc, &mut fdest)
        .with_context(|| format!("copying {fpath:?} to {fname:?}"))?;

    Ok(fname.into())
}
