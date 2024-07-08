use crate::{
    collector::{Collector, PacketTimeIter},
    config::{get_default, Config},
    time::{time_decoder, LeapSecs},
    writer,
};
use anyhow::{bail, Context, Result};
use chrono::Utc;
use crossbeam::channel;
use std::{
    fs::{create_dir, File},
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
    thread,
};
use tempfile::TempDir;
use tracing::{debug, error, info, warn};

fn get_config(satellite: Option<String>, fpath: Option<PathBuf>) -> Result<Config> {
    match satellite {
        Some(satid) => get_default(&satid).context("getting default config"),
        None => Config::with_path(&fpath.unwrap()).context("Invalid config"),
    }
}

pub fn create_rdr<P>(
    config: &Config,
    leap_seconds: LeapSecs,
    packet_groups: P,
    dest: &Path,
) -> Result<()>
where
    P: Iterator<Item = ccsds::PacketGroup> + Send,
{
    let decode_iet = time_decoder(leap_seconds).context("initializing time decoder")?;
    let mut collector = Collector::new(config.satellite.clone(), &config.rdrs, &config.products);

    if !dest.exists() {
        create_dir(dest)?;
    }

    let (tx, rx) = channel::unbounded();
    thread::scope(|s| {
        s.spawn(move || {
            for (pkt, pkt_utc, pkt_iet) in PacketTimeIter::new(packet_groups, decode_iet) {
                let complete = collector.add(pkt_utc, pkt_iet, pkt);
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
            let created = Utc::now();
            for rdrs in rx {
                match writer::write_hdf5(config, &rdrs, created, dest).context("writing h5") {
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
    let paths: Vec<String> = paths
        .iter()
        .map(|p| p.as_ref().to_string_lossy().to_string())
        .collect();
    let dest = dest.as_ref();
    let writer = BufWriter::new(
        File::create(dest).with_context(|| format!("creating merge dest file: {dest:?}"))?,
    );
    Ok(
        ccsds::merge_by_timecode(&paths, &ccsds::CDSTimeDecoder, writer)
            .context("merging spacepackets")?,
    )
}

const LEAPSEC_DOWNLOAD_URL: &str = "https://hpiers.obspm.fr/iers/bul/bulc/ntp/leap-seconds.list";

pub fn create(
    satellite: Option<String>,
    config: Option<PathBuf>,
    leap_seconds: Option<PathBuf>,
    input: &[PathBuf],
    output: PathBuf,
) -> Result<()> {
    let config = get_config(satellite, config)?;
    for input in input {
        if !input.exists() {
            bail!("Input does not exist: {input:?}");
        }
    }

    let leaps = LeapSecs::new(leap_seconds.as_deref()).context("creating leapsecs db")?;
    info!("leap seconds {leaps}");
    if leaps.expired {
        warn!(
            "leap seconds db is expired. Consider downloading an updated \
               one from the following URL and providing the --leap-seconds <path> flag: {}",
            LEAPSEC_DOWNLOAD_URL
        );
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
    let groups = ccsds::read_packet_groups(file).flatten();

    create_rdr(&config, leaps, groups, &output)?;

    if let Some(dir) = tmpdir {
        debug!(dir = ?dir.path(), "removing tempdir");
        dir.close()?;
    }

    Ok(())
}
