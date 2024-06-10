use std::{
    collections::HashMap,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use hdf5::{File as H5File, Group};
use ndarray::s;
use tempfile::TempDir;
use tracing::{debug, info, warn};

const SUPPORTED_SENSORS: [&str; 4] = ["VIIRS", "CRIS", "ATMS", "OMPS"];

enum DatasetType<'a> {
    Science(&'a str),
    Spacecraft(u16),
}

fn dataset_name(scid: u8, type_: &DatasetType, created: DateTime<Utc>) -> String {
    let dstr = created.format("%y%j%H%M%S");
    match type_ {
        DatasetType::Science(path) => {
            if path.contains("VIIRS") {
                format!("P{scid:03}0826VIIRSSCIENCEAS{dstr}001.PDS")
            } else if path.contains("CRIS") {
                format!("P{scid:03}1289CRISSCIENCEAAS{dstr}001.PDS")
            } else if path.contains("ATMS") {
                format!("P{scid:03}0515ATMSSCIENCEAAS{dstr}001.PDS")
            } else if path.contains("OMPS") {
                format!("P{scid:03}????OMPSSCIENCEAAS{dstr}001.PDS")
            } else {
                format!("{scid:03}-{dstr}.dat")
            }
        }
        DatasetType::Spacecraft(apid) => {
            format!("P{scid:03}{apid:04}AAAAAAAAAAAAAS{dstr}001.PDS")
        }
    }
}

/// Dump the Common RDR Application Packets Storage to a file.
fn dump_datasets_to(workdir: &Path, path: &str, group: &Group) -> Result<Vec<PathBuf>> {
    let mut files = Vec::default();

    for (idx, dataset) in group
        .datasets()
        .context("Getting group datasets")?
        .iter()
        .enumerate()
    {
        let bytes = dataset.read_1d::<u8>().context("Reading data")?;
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
        fs::write(&destpath, packet_data).with_context(|| format!("Writing to {destpath:?}"))?;

        files.push(destpath.clone());
    }

    Ok(files)
}

fn dump_group(
    workdir: &Path,
    scid: u8,
    path: &str,
    group: &Group,
    created: DateTime<Utc>,
) -> Result<Option<PathBuf>> {
    info!("dumping {path} to {workdir:?}");
    let files = dump_datasets_to(workdir, path, group)?;
    if files.is_empty() {
        return Ok(None);
    }
    let destpath = workdir.join(dataset_name(scid, &DatasetType::Science(path), created));
    debug!("merging {} files to {destpath:?}", files.len());
    let dest = File::create(&destpath).with_context(|| format!("Creating {destpath:?}"))?;
    ccsds::merge_by_timecode(&files, &ccsds::CDSTimeDecoder, dest)
        .with_context(|| format!("Merging {} files", files.len()))?;

    Ok(Some(destpath))
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

pub fn split_spacecraft(fpath: &Path, scid: u8, created: DateTime<Utc>) -> Result<Vec<PathBuf>> {
    let mut files: HashMap<u16, File> = HashMap::default();
    let mut paths: Vec<PathBuf> = Vec::default();

    for packet in ccsds::read_packets(&File::open(fpath)?) {
        if let Err(err) = packet {
            bail!("error while reading packets: {err}");
        }
        let packet = packet.unwrap();

        let dest = files.entry(packet.header.apid).or_insert_with(|| {
            let sc_path = fpath.with_file_name(dataset_name(
                scid,
                &DatasetType::Spacecraft(packet.header.apid),
                created,
            ));
            debug!("creating {sc_path:?}!");
            paths.push(sc_path.clone());
            File::create(&sc_path).expect("could not create destination")
        });

        dest.write_all(&packet.data)?;
    }

    Ok(paths)
}

pub fn dump(input: &Path, spacecraft: bool) -> Result<()> {
    if !input.is_file() {
        bail!("Failed to open {input:?}");
    }
    let scid = get_spacecraft(input);
    let workdir = TempDir::new()?;
    let created = Utc::now();

    let file = H5File::open(input).context("Opening input")?;

    let mut groups = Vec::default();
    for sensor in SUPPORTED_SENSORS {
        let path = format!("All_Data/{sensor}-SCIENCE-RDR_All");
        groups.push(path);
    }
    if spacecraft {
        groups.push("All_Data/SPACECRAFT-DIARY-RDR_All".to_string());
    }

    for group_path in groups {
        debug!("trying to dump {group_path}");
        if let Ok(group) = file.group(&group_path) {
            let dat_path = dump_group(workdir.path(), scid, &group_path, &group, created)?;
            if dat_path.is_none() {
                warn!("no data found for {group_path}");
                continue;
            }
            let dat_path = dat_path.unwrap();

            if spacecraft && group_path.contains("SPACECRAFT") {
                debug!("splitting {dat_path:?} into separate spacecraft files");
                let files = split_spacecraft(&dat_path, scid, created)
                    .context("splitting spacecraft files")?;
                for fpath in files {
                    let dest = fpath.file_name().unwrap();
                    fs::rename(&fpath, dest)
                        .with_context(|| format!("renaming {dat_path:?} to {dest:?}"))?;
                    info!("wrote {dest:?}");
                }
            } else {
                let dest = dat_path.file_name().unwrap();
                fs::rename(&dat_path, dest)
                    .with_context(|| format!("renaming {dat_path:?} to {dest:?}"))?;
                info!("wrote {dest:?}");
            }
        } else {
            debug!("Failed to open {group_path}, assuming it does not exist");
        }
    }

    Ok(())
}
