use anyhow::{bail, Context, Result};
use ccsds::read_packet_groups;
use rdr::{
    collector::{Collector, PacketTimeIter},
    config::{get_default, Config},
    merge,
    time::time_decoder,
    writer::write_hdf5,
};
use std::{
    fs::{create_dir, File},
    path::PathBuf,
};
use tempfile::TempDir;
use tracing::{debug, info};

fn get_config(satellite: Option<String>, fpath: Option<PathBuf>) -> Result<Config> {
    match satellite {
        Some(satid) => get_default(&satid).context("getting default config"),
        None => Config::with_path(&fpath.unwrap()).context("Invalid config"),
    }
}

pub fn create(
    satellite: Option<String>,
    config: Option<PathBuf>,
    leap_seconds: &Option<PathBuf>,
    input: &[PathBuf],
) -> Result<()> {
    let config = get_config(satellite, config)?;

    // Validate the inputs
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
        merge::merge(input, dest.clone()).context("merging multiple inputs")?;
        tmpdir = Some(dir);
        dest
    } else {
        input[0].clone()
    };

    let fin = File::open(input.clone()).with_context(|| format!("opening input: {input:?}"))?;
    let groups = read_packet_groups(fin).filter_map(Result::ok);
    let decode_iet =
        time_decoder(leap_seconds.as_deref()).context("initializing leap-seconds db")?;

    let mut collector = Collector::new(config.satellite.clone(), &config.rdrs, &config.products);

    let dest = PathBuf::from("output");
    if !dest.exists() {
        create_dir(&dest)?;
    }

    for (pkt, pkt_utc, pkt_iet) in PacketTimeIter::new(groups, decode_iet) {
        let complete = collector.add(pkt_utc, pkt_iet, pkt);

        if let Some((rdr, packed)) = complete {
            debug!(
                "collected RDR {}/{} granule={}",
                rdr.header.satellite, rdr.product.short_name, rdr.granule_time,
            );

            let fpath = write_hdf5(&config, &rdr, &packed, &dest).context("writing h5")?;
            info!("wrote {fpath:?}");
        }
    }

    for (rdr, packed) in collector.finish() {
        debug!(
            "collected RDR {}/{} granule={}",
            rdr.header.satellite, rdr.product.short_name, rdr.granule_time,
        );
    }

    if let Some(dir) = tmpdir {
        debug!(dir = ?dir.path(), "removing tempdir");
        dir.close()?;
    }

    Ok(())
}
