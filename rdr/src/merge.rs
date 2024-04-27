use anyhow::{Context, Result};
use ccsds::{merge_by_timecode, CDSTimeDecoder};
use std::{fs::File, io::BufWriter, path::Path};

pub fn merge<P: AsRef<Path>>(paths: &[P], dest: P) -> Result<()> {
    let paths: Vec<String> = paths
        .iter()
        .map(|p| p.as_ref().to_string_lossy().to_string())
        .collect();
    let dest = dest.as_ref();
    let writer = BufWriter::new(
        File::create(dest).with_context(|| format!("creating merge dest file: {dest:?}"))?,
    );
    merge_by_timecode(&paths, &CDSTimeDecoder, writer).context("merging")
}
