use anyhow::{bail, Context, Result};
use ccsds::{CDSTimeDecoder, Packet, TimeDecoder};
use std::{
    fmt::Display,
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use leap_seconds::{Date, DateTime, LeapSecondsList, Time, Timestamp};

// Static leap-second.list file embeded in build.rs
// Obtain a new one from https://hpiers.obspm.fr/iers/bul/bulc/ntp/leap-seconds.list
// NOTE: The build will bail if this file is too old. See build.rs.
const LEAP_SECONDS_LIST: &str = include_str!(concat!(env!("OUT_DIR"), "/leap-seconds.list"));
// Number of microseconds between IET and UTC epochs
const EPOCH_OFFSET: u64 = 378_691_200_000_000;

#[derive(Debug, Clone)]
struct Leap {
    // UTC seconds for when the update ocurred
    time: u64,
    // Number of leap seconds added or removed at this time.
    num: i32,
}

pub struct LeapSecs {
    leaps: Vec<Leap>,
    pub updated: u64,
    pub expired: bool,
}

impl Default for LeapSecs {
    fn default() -> Self {
        LeapSecs::with_reader(BufReader::new(LEAP_SECONDS_LIST.as_bytes()), true).unwrap()
    }
}

impl Display for LeapSecs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LeapSecs{{updated:{}, expired:{}}}",
            self.updated, self.expired
        )
    }
}

impl LeapSecs {
    pub fn new(path: Option<&Path>) -> Result<Self> {
        match path {
            Some(path) => {
                let reader = BufReader::new(File::open(path)?);
                LeapSecs::with_reader(reader, true)
            }
            None => Ok(LeapSecs::default()),
        }
    }

    fn with_reader<R: BufRead>(reader: R, ignore_expired: bool) -> Result<Self> {
        let list = LeapSecondsList::new(reader).context("opening leapsec file")?;
        if !ignore_expired && list.is_expired() {
            let exp = list.expiration_date();
            bail!("leap-seconds db is expired: expiration={exp}")
        }

        let utc_offset = Timestamp::from_date_time(DateTime {
            date: Date::new(1970, 1, 1).expect("valid date"),
            time: Time::new(0, 0, 0).expect("valid time"),
        })
        .expect("valid datetime")
        .as_u64();

        //
        let mut leaps = Vec::new();
        let mut lastval: i32 = 0;
        for leap in list.leap_seconds() {
            let cur = i32::from(leap.tai_diff());
            let diff: i32 = if lastval == 0 { cur } else { cur - lastval };
            lastval = cur;
            leaps.push(Leap {
                // convert from epoch 1900-01-01 to 1970-01-01
                time: leap.timestamp().as_u64() - utc_offset,
                num: diff,
            });
        }

        Ok(LeapSecs {
            leaps,
            updated: list.last_update().as_u64() - utc_offset,
            expired: list.is_expired(),
        })
    }

    /// Number of leap microseconds between UTC and IET at the give UTC time in microseconds.
    fn leap_usecs(&self, usecs: u64) -> u64 {
        let usecs = usecs / 1000 / 1000;
        let mut leapsecs = 0;

        self.leaps.iter().for_each(|leap| {
            if leap.time <= usecs {
                leapsecs += leap.num;
            }
        });

        if leapsecs < 0 {
            0
        } else {
            leapsecs as u64 * 1000 * 1000
        }
    }

    /// Convert UTC microseconds to IET microseconds.
    pub fn utc_to_iet(&self, usecs: u64) -> u64 {
        usecs + EPOCH_OFFSET + self.leap_usecs(usecs)
    }
}

/// Function that decodes a packets UTC and IET times
pub type TimeFcn = dyn Fn(&Packet) -> (u64, u64);

pub fn time_decoder(leaps_list: Option<&Path>) -> Result<Box<TimeFcn>> {
    let leaps = LeapSecs::new(leaps_list)?;
    let decoder = CDSTimeDecoder;

    let fcn = move |pkt: &Packet| -> (u64, u64) {
        let utc = decoder.decode_time(pkt).unwrap_or(0);
        (utc, leaps.utc_to_iet(utc))
    };

    Ok(Box::new(fcn))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leapsdb_leap_usecs() {
        let db = LeapSecs::default();

        // First 10s of leapseconds added 1972-01-01
        let leapsecs = db.leap_usecs(63_072_000_000_000);

        assert_eq!(leapsecs, 10_000_000);
    }

    #[test]
    fn utc_to_iet() {
        let db = LeapSecs::default();

        // Jan 1, 2018
        let usecs = 1_514_764_800_000_000;

        let zult = db.utc_to_iet(usecs);

        assert_eq!(zult, 1_893_456_037_000_000);
    }
}
