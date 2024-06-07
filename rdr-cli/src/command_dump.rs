use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use hdf5::{File as H5File, Group};
use ndarray::s;
use tempfile::TempDir;
use tracing::{debug, info};

const SUPPORTED_SENSORS: [&str; 4] = ["VIIRS", "CRIS", "ATMS", "OMPS"];

fn path_to_name(scid: u8, path: &str, created: DateTime<Utc>) -> String {
    let dstr = created.format("%Y%m%d%H%M%S");
    if path.contains("VIIRS") {
        format!("P{scid:03}0826VIIRSSCIENCEAS{dstr}01.PDS")
    } else if path.contains("CRIS") {
        format!("P{scid:03}1289CRISSCIENCEAAS{dstr}01.PDS")
    } else if path.contains("ATMS") {
        format!("P{scid:03}0515ATMSSCIENCEAAS{dstr}01.PDS")
    } else if path.contains("OMPS") {
        format!("P{scid:03}????OMPSSCIENCEAAS{dstr}01.PDS")
    } else {
        format!("{scid}-{dstr}.dat")
    }
}

fn dump_datasets_to(workdir: &Path, path: &str, group: &Group) -> Result<Vec<PathBuf>> {
    let mut files = Vec::default();

    for (idx, dataset) in group.datasets()?.iter().enumerate() {
        let bytes = dataset.read_1d::<u8>()?;
        debug!("{path} dimension {}", bytes.dim());

        let ap_offset = {
            let x = bytes
                .slice(s![48..52])
                .to_slice()
                .context("getting static header apStorageOffset")?;
            u32::from_be_bytes([x[0], x[1], x[2], x[3]])
        };
        let ap_end = {
            let x = bytes
                .slice(s![52..56])
                .to_slice()
                .context("getting static header nextPktPos")?;
            u32::from_be_bytes([x[0], x[1], x[2], x[3]])
        };

        debug!("{path} packet data apStorageOffset={ap_offset} nextPktPos={ap_end}");
        let packet_data = bytes
            .slice(s![ap_offset as usize..ap_end as usize])
            .to_slice()
            .context("reading packet data")?;

        let destpath = workdir
            .join(path.replace('/', "::"))
            .with_extension(format!("{idx}"));
        debug!("writing to {destpath:?}");
        fs::write(&destpath, packet_data)?;

        files.push(destpath.clone());
    }

    Ok(files)
}

fn dump_group(workdir: &Path, scid: u8, path: &str, group: &Group) -> Result<PathBuf> {
    info!("dumping {path} to {workdir:?}");
    let files = dump_datasets_to(workdir, path, group)?;
    let created: DateTime<Utc> = Utc::now();
    let destpath = workdir.join(path_to_name(scid, path, created));
    info!("merging {} files to {destpath:?}", files.len());
    let dest = File::create(&destpath)?;
    ccsds::merge_by_timecode(&files, &ccsds::CDSTimeDecoder, dest).context("merging")?;

    Ok(destpath)
}

fn get_spacecraft(path: &Path) -> u8 {
    let path = path.to_string_lossy();
    if path.contains("npp") {
        157
    } else if path.contains("j01") {
        159
    } else if path.contains("j02") {
        177
    } else if path.contains("j03") {
        178
    } else if path.contains("j04") {
        179
    } else {
        0
    }
}

pub fn dump(input: PathBuf, spacecraft: bool) -> Result<()> {
    let scid = get_spacecraft(&input);
    let workdir = TempDir::new()?;

    let file = H5File::open(input)?;

    let mut groups = Vec::default();
    for sensor in SUPPORTED_SENSORS {
        let path = format!("All_Data/{sensor}-SCIENCE-RDR_All");
        groups.push(path);
    }
    if spacecraft {
        groups.push("All_Data/SPACECRAFT-DIARY-RDR_All".to_owned());
    }

    for path in groups {
        debug!("trying to dump {path}");
        if let Ok(group) = file.group(&path) {
            info!("dumping {path} to {:?}", workdir.path());
            let path = dump_group(workdir.path(), scid, &path, &group)?;
            let dest = path.file_name().unwrap();
            fs::rename(&path, path.file_name().unwrap())
                .with_context(|| format!("renaming {path:?} to {dest:?}"))?;
        } else {
            debug!("{path} does not exist, skipping");
        }
    }

    Ok(())
}
