use anyhow::{Context, Result};
use hdf5::types::FixedAscii;
use rdr::CommonRdr;
use std::fs::{write, File};
use std::path::{Path, PathBuf};
use tracing::{debug, warn};

pub struct ExtractedOutput {
    pub path: PathBuf,
    pub granule_id: String,
    pub short_name: String,
}

pub fn extract<I: AsRef<Path>, O: AsRef<Path>>(
    input: I,
    outdir: O,
    short_name: Option<String>,
    granule_id: Option<String>,
) -> Result<Vec<ExtractedOutput>> {
    let mut outputs = Vec::default();

    let outdir = outdir.as_ref();
    std::fs::create_dir_all(outdir).with_context(|| format!("creating direcotry {outdir:?}"))?;

    let file = hdf5::File::open(&input)
        .with_context(|| format!("failed to open {:?}", input.as_ref().to_path_buf()))?;

    let all_data = file.group("All_Data").context("failed to open /All_Data")?;
    for group in all_data
        .groups()
        .context("failed to get /All_Data groups")?
    {
        if let Some(short_name) = short_name.as_ref() {
            if !group.name().ends_with(&format!("{short_name}_All")) {
                debug!("skipping group {}", group.name());
                continue;
            }
        }
        for dataset in group
            .datasets()
            .with_context(|| format!("failed to get {} groups", group.name()))?
        {
            let dataset_path = dataset.name();
            let short_name = dataset_path
                .split("/")
                .nth(2)
                .unwrap_or_default()
                .replace("_All", "");
            if short_name.is_empty() {
                warn!("failed to parse short name from {dataset_path}");
                continue;
            }
            let id = get_granule_id(&file, &dataset_path)?;

            if let Some(granule_id) = granule_id.as_ref() {
                if id != *granule_id {
                    debug!("skipping granule {short_name} {id}");
                    continue;
                }
            }

            // read entire common rdr data bytes
            let arr = dataset
                .read_1d::<u8>()
                .with_context(|| format!("reading {}", dataset.name()))?;
            let Some(data) = arr.as_slice() else {
                warn!("invalid array format for {short_name}");
                continue;
            };

            let common_rdr = CommonRdr::from_bytes(data)?;
            let fpfx = format!("{short_name}_{id}");
            let fpath = outdir.join(format!("{fpfx}.json"));
            let file = File::create(&fpath).with_context(|| format!("creating {fpath:?}"))?;
            serde_json::to_writer_pretty(&file, &common_rdr)?;

            let fpath = outdir.join(format!("{fpfx}.dat"));
            write(&fpath, data).with_context(|| format!("writing {fpath:?}"))?;

            outputs.push(ExtractedOutput {
                path: fpath,
                granule_id: id,
                short_name,
            });
        }
    }

    Ok(outputs)
}

fn get_granule_id(file: &hdf5::File, dataset_path: &str) -> Result<String> {
    let gran_num: u64 = dataset_path.split("_").last().unwrap_or_default().parse()?;
    let short_name = dataset_path
        .split("/")
        .nth(2)
        .unwrap_or_default()
        .replace("_All", "");
    let path = format!("Data_Products/{short_name}/{short_name}_Gran_{gran_num}");

    let dataset = file
        .dataset(&path)
        .with_context(|| format!("opening dataset {path}"))?;
    let attr = dataset
        .attr("N_Granule_ID")
        .context("getting attr {path}:N_Granule_ID")?;
    Ok(attr
        .read_2d::<FixedAscii<20>>()
        .context("reading attr {path}:N_Granule_ID")?[[0, 0]]
    .to_string())
}
