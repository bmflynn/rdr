use anyhow::{bail, Context, Result};
use ccsds::spacepacket::{collect_groups, decode_packets, PacketGroup};
use crossbeam::channel;
use rdr::{
    config::{get_default, Config},
    jpss_merge, write_hdf5, Collector, PacketTimeIter, Time,
};
use std::{
    fs::{create_dir, File},
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
    thread,
};
use tempfile::TempDir;
use tracing::{debug, error, info};

fn get_config(satellite: Option<String>, fpath: Option<PathBuf>) -> Result<Option<Config>> {
    match satellite {
        Some(satid) => get_default(&satid).context("getting default config"),
        None => Ok(Some(
            Config::with_path(&fpath.unwrap()).context("Invalid config")?,
        )),
    }
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
                let complete = collector.add(&pkt_time, pkt);
                if let Some(rdrs) = complete {
                    debug!("collected {}", &rdrs[0]);
                    let _ = tx.send(rdrs);
                }
            }
            for rdrs in collector.finish() {
                debug!("collected {}", &rdrs[0]);
                let _ = tx.send(rdrs);
            }
        });

        s.spawn(move || {
            let created = Time::now();
            for rdrs in rx {
                match write_hdf5(config, &rdrs, &created, dest) {
                    Ok(fpath) => info!("wrote {} to {fpath:?}", &rdrs[0]),
                    Err(err) => {
                        error!("failed writing {}: {err}", &rdrs[0]);
                    }
                };
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
