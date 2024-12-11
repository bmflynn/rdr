use std::str::FromStr;
use std::{io::Write, path::PathBuf};

use ccsds::spacepacket::{Merger, TimecodeDecoder};
use ccsds::Result;
use hifitime::efmt::{Format, Formatter};
use hifitime::Epoch;

pub fn jpss_merge<W: Write>(files: &[PathBuf], writer: W) -> Result<()> {
    let time_decoder = TimecodeDecoder::new(ccsds::timecode::Format::Cds {
        num_day: 2,
        num_submillis: 2,
    });

    Merger::new(files.to_vec(), time_decoder)
        .with_apid_order(&[826, 821])
        .merge(writer)
}

pub fn now() -> Epoch {
    Epoch::now().expect("failed to get system time")
}

pub fn format_epoch(epoch: Epoch, fmt: &str) -> String {
    let fmt = Format::from_str(fmt).unwrap();
    let formatter = Formatter::new(epoch, fmt);
    format!("{formatter}")
}
