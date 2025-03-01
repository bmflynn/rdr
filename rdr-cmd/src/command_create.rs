use anyhow::{bail, Context, Result};
use ccsds::spacepacket::{collect_groups, decode_packets, PacketGroup};
use crossbeam::channel;
use rdr::{
    config::{get_default, Config},
    jpss_merge, Collector, Meta, PacketTimeIter, Rdr, Time,
};
use std::{
    collections::{HashMap, HashSet},
    fs::{create_dir, File},
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
    thread,
};
use tempfile::TempDir;
use tracing::{debug, error, info, warn};

fn get_config(satellite: Option<String>, fpath: Option<PathBuf>) -> Result<Option<Config>> {
    match (satellite, fpath) {
        (Some(satid), None) | (Some(satid), Some(_)) => Ok(get_default(&satid)),
        (None, Some(fpath)) => Ok(Some(Config::with_path(&fpath).context("Invalid config")?)),
        (None, None) => bail!("One of satellite or path is required to get config"),
    }
}

pub fn rdr_filename_meta(rdrs: &[Rdr]) -> (Time, Time, Vec<String>) {
    assert!(!rdrs.is_empty());
    let mut start = Time::now().iet();
    let mut end = 0;
    let mut product_ids: HashSet<String> = HashSet::default();
    for rdr in rdrs {
        // Only science types determine file time. There should only be one science type but we
        // leave that to the caller and just compute times based on all science types.
        if rdr.meta.collection.contains("SCIENCE") {
            start = std::cmp::min(start, rdr.meta.begin_time_iet);
            end = std::cmp::max(end, rdr.meta.end_time_iet);
        }
        product_ids.insert(rdr.product_id.to_string());
    }
    let mut product_ids = Vec::from_iter(product_ids);
    product_ids.sort();

    (Time::from_iet(start), Time::from_iet(end), product_ids)
}

pub fn create_rdr<P>(config: &Config, packet_groups: P, dest: &Path) -> Result<()>
where
    P: Iterator<Item = PacketGroup> + Send,
{
    let mut collector = Collector::new(config.satellite.clone(), &config.rdrs, &config.products);

    if !dest.exists() {
        create_dir(dest)?;
    }

    let (tx, rx) = channel::unbounded();
    thread::scope(|s| {
        s.spawn(move || {
            for (pkt, pkt_time) in PacketTimeIter::new(packet_groups) {
                let complete = match collector.add(&pkt_time, pkt) {
                    Ok(o) => o,
                    Err(e) => {
                        warn!("failed to add packet: {e}");
                        continue;
                    }
                };
                if let Some(rdrs) = complete {
                    let mut counts: HashMap<String, usize> = HashMap::default();
                    for r in &rdrs {
                        *counts.entry(r.meta.collection.to_string()).or_default() += 1;
                    }
                    debug!("collected RDR {:?} {:?}", &rdrs[0].meta.begin, counts);
                    let _ = tx.send(rdrs);
                }
            }
            for rdrs in collector.finish().expect("finishing collection") {
                let mut counts: HashMap<String, usize> = HashMap::default();
                for r in &rdrs {
                    *counts.entry(r.meta.collection.to_string()).or_default() += 1;
                }
                debug!("collected RDR {:?} {:?}", &rdrs[0].meta.begin, counts);
                let _ = tx.send(rdrs);
            }
        });

        s.spawn(move || {
            let created = Time::now();
            for rdrs in rx {
                let (start, end, pids) = rdr_filename_meta(&rdrs);
                let fpath = dest.join(rdr::filename(
                    &config.satellite.id,
                    &config.origin,
                    &config.mode,
                    &created,
                    &start,
                    &end,
                    &pids,
                ));
                let short_names: Vec<String> =
                    rdrs.iter().map(|r| r.meta.collection.to_string()).collect();
                let Some(meta) = Meta::from_products(&short_names, config) else {
                    warn!(
                        "RDR generated with one or more unknown product ids: {:?}",
                        short_names
                    );
                    continue;
                };
                match rdr::create_rdr(&fpath, meta, &rdrs) {
                    Ok(_) => info!("wrote {} to {fpath:?}", &rdrs[0]),
                    Err(err) => error!("failed to write {fpath:?}: {err}"),
                }
            }
        });
    });

    Ok(())
}

pub fn merge<P: AsRef<Path>>(paths: &[P], dest: P) -> Result<()> {
    let paths: Vec<PathBuf> = paths.iter().map(|p| p.as_ref().to_path_buf()).collect();
    let dest = dest.as_ref();
    let writer = BufWriter::new(
        File::create(dest).with_context(|| format!("creating merge dest file: {dest:?}"))?,
    );
    Ok(jpss_merge(&paths, writer)?)
}

pub fn create(
    satellite: Option<String>,
    config: Option<PathBuf>,
    input: &[PathBuf],
    output: PathBuf,
) -> Result<()> {
    let config = match get_config(satellite, config) {
        Ok(Some(config)) => config,
        Ok(None) => bail!("No spacecraft configuration found"),
        Err(err) => bail!("Failed to lookup config: {err}"),
    };
    for input in input {
        if !input.exists() {
            bail!("Input does not exist: {input:?}");
        }
    }

    // Get single input, merging multiple inputs if necessary
    let mut tmpdir: Option<TempDir> = None;
    let input = if input.len() > 1 {
        let dir = TempDir::new()?;
        let dest = dir.path().join("merge.dat");
        info!(?input, ?dest, "merging inputs");
        merge(input, dest.clone()).context("merging multiple inputs")?;
        tmpdir = Some(dir);
        dest
    } else {
        input[0].clone()
    };
    let file = BufReader::new(File::open(input)?);
    let packets = decode_packets(file).filter_map(Result::ok);
    let groups = collect_groups(packets).filter_map(Result::ok);

    create_rdr(&config, groups, &output)?;

    if let Some(dir) = tmpdir {
        debug!(dir = ?dir.path(), "removing tempdir");
        dir.close()?;
    }

    Ok(())
}
