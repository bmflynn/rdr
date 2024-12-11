use std::{io::Write, path::PathBuf};

use ccsds::spacepacket::{Merger, TimecodeDecoder};
use ccsds::Result;

pub fn jpss_merge<W: Write>(files: &[PathBuf], writer: W) -> Result<()> {
    let time_decoder = TimecodeDecoder::new(ccsds::timecode::Format::Cds {
        num_day: 2,
        num_submillis: 2,
    });

    Merger::new(files.to_vec(), time_decoder)
        .with_apid_order(&[826, 821])
        .merge(writer)
}
