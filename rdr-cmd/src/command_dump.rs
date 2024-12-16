use anyhow::{bail, Context, Result};
use ccsds::spacepacket::decode_packets;
use hdf5::{File as H5File, Group};
use rdr::{jpss_merge, ApidInfo, PacketTracker, StaticHeader, Time};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};
use tempfile::TempDir;
use tracing::{debug, info, trace, warn};

const SUPPORTED_SENSORS: [&str; 4] = ["VIIRS", "CRIS", "ATMS", "OMPS"];

enum DatasetType<'a> {
    Science(&'a str),
    Spacecraft(u16),
}

fn dataset_name(scid: u8, type_: &DatasetType, created: &Time) -> String {
    let dstr = created.format_utc("%y%j%H%M%S");
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

const NO_PACKETS_RECEIVED: i32 = -1;

/// Dump the Common RDR Application Packets Storage to a file.
fn dump_datasets_to(workdir: &Path, path: &str, group: &Group) -> Result<Vec<PathBuf>> {
    let mut files = Vec::default();

    for (idx, dataset) in group
        .datasets()
        .context("Getting group datasets")?
        .iter()
        .enumerate()
    {
        let destpath = workdir
            .join(path.replace('/', "::"))
            .with_extension(format!("{idx}"));
        debug!("writing to {destpath:?}");
        let mut file = File::create(&destpath).context("opening packet dest file")?;

        // The whole common RDR as bytes
        let bytes = dataset.read_1d::<u8>().context("Reading data")?;
        let data = bytes.as_slice().context("converting to slice")?;

        let header = StaticHeader::from_bytes(data).context("decoding static header")?;
        trace!("{header:?}");

        let start = header.apid_list_offset as usize;
        let end = start + ApidInfo::LEN * usize::try_from(header.num_apids)?;
        let apids = ApidInfo::all_from_bytes(&data[start..end]).context("decoding apidlist")?;

        debug!("{path} num_apids={}", apids.len());

        for apid in &apids {
            debug!(
                "reading {}({}) pkts_received={}",
                apid.name, apid.value, apid.pkts_received
            );
            trace!("{:?}", apid);

            let mut tracker_offset = header.pkt_tracker_offset as usize
                + apid.pkt_tracker_start_idx as usize * PacketTracker::LEN;
            for _ in 0..apid.pkts_received {
                let tracker = PacketTracker::from_bytes(&data[tracker_offset..])
                    .context("decoding packet tracker")?;
                trace!("{:?}", tracker);
                tracker_offset += PacketTracker::LEN;
                if tracker.offset == NO_PACKETS_RECEIVED {
                    break;
                }
                let start = header.ap_storage_offset as usize + usize::try_from(tracker.offset)?;
                let end = start + usize::try_from(tracker.size)?;
                file.write_all(&data[start..end])?;
            }
        }

        files.push(destpath.clone());
    }

    Ok(files)
}

fn dump_group(
    workdir: &Path,
    scid: u8,
    path: &str,
    group: &Group,
    created: &Time,
) -> Result<Option<PathBuf>> {
    info!("dumping {path} to {workdir:?}");
    let files = dump_datasets_to(workdir, path, group)?;
    if files.is_empty() {
        return Ok(None);
    }
    let destpath = workdir.join(dataset_name(scid, &DatasetType::Science(path), created));
    debug!("merging {} files to {destpath:?}", files.len());
    let dest = File::create(&destpath).with_context(|| format!("Creating {destpath:?}"))?;

    jpss_merge(&files, dest).with_context(|| format!("Merging {} files", files.len()))?;

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

pub fn split_spacecraft(fpath: &Path, scid: u8, created: &Time) -> Result<Vec<PathBuf>> {
    let mut files: HashMap<u16, File> = HashMap::default();
    let mut paths: Vec<PathBuf> = Vec::default();

    for packet in decode_packets(&File::open(fpath)?) {
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
    let created = Time::now();

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
            let dat_path = dump_group(workdir.path(), scid, &group_path, &group, &created)?;
            if dat_path.is_none() {
                warn!("no data found for {group_path}");
                continue;
            }
            let dat_path = dat_path.unwrap();

            if spacecraft && group_path.contains("SPACECRAFT") {
                debug!("splitting {dat_path:?} into separate spacecraft files");
                let files = split_spacecraft(&dat_path, scid, &created)
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
