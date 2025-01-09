use std::ops::Deref;
use std::str::FromStr;

use hifitime::efmt::{Format, Formatter};
use hifitime::{Epoch, TimeScale};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Time(Epoch);

impl AsRef<Epoch> for Time {
    fn as_ref(&self) -> &Epoch {
        &self.0
    }
}

impl Deref for Time {
    type Target = Epoch;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Time {
    // Difference betweeh hifitime epoch (1900-01-01) and JPSS epoch (Jan 1, 1958) in microseconds
    const IET_DELTA: u64 = 1_830_297_600_000_000;

    pub fn now() -> Self {
        Time(
            Epoch::now()
                .expect("failed to get system time")
                .to_time_scale(TimeScale::TAI),
        )
    }

    pub fn from_epoch(epoch: Epoch) -> Self {
        Time(epoch.to_time_scale(TimeScale::TAI))
    }

    /// Create [Time] from UTC microseconds since Jan 1, 1970.
    pub fn from_utc(micros: u64) -> Self {
        Self(Epoch::from_unix_milliseconds((micros / 1_000) as f64).to_time_scale(TimeScale::TAI))
    }

    /// Create [Time] from IET microseconds.
    pub fn from_iet(micros: u64) -> Self {
        Self(Epoch::from_tai_seconds(
            (micros + Self::IET_DELTA) as f64 / 1_000_000.0,
        ))
    }

    /// Return UTC microseconds since Jan 1, 1970
    pub fn utc(&self) -> u64 {
        self.0.to_unix_milliseconds() as u64 * 1000
    }
    /// Return TAI microseconds since Jan 1, 1958
    pub fn iet(&self) -> u64 {
        self.0.to_tai(hifitime::Unit::Microsecond) as u64 - Self::IET_DELTA
    }

    /// Format ourself using the provided format string.
    ///
    /// See [hifitime::efmt::Format].
    pub fn format_utc(&self, fmt: &str) -> String {
        let fmt = Format::from_str(fmt).unwrap();
        let formatter = Formatter::to_time_scale(self.0, fmt, hifitime::TimeScale::UTC);
        format!("{formatter}")
    }
}

#[cfg(test)]
mod test {
    use hifitime::Unit;

    use super::*;

    #[test]
    fn test_format() {
        let time = Time(Epoch::from_unix_seconds(0.0));

        assert_eq!(
            time.format_utc("%Y-%m-%dT%H:%M:%S%z"),
            "1970-01-01T00:00:00+00:00"
        );
    }

    #[test]
    fn test_utc() {
        let time = Time(Epoch::from_unix_seconds(0.0));

        assert_eq!(time.utc(), 0);
    }

    #[test]
    fn test_iet() {
        let time = Time(Epoch::from_unix_seconds(0.0));

        assert_eq!(time.iet(), 378_691_200_000_000);
    }

    #[test]
    fn test_from_iet() {
        let iet: u64 = 2112504609700000;
        assert_eq!(Time::from_iet(iet).iet(), iet);
    }

    #[test]
    fn test_hifitime() {
        let epoch = Epoch::from_str("1970-01-01T00:00:00Z").unwrap();
        eprintln!(
            "time:{epoch:?} scale:{} tai:{} utc:{}",
            epoch.time_scale,
            epoch.to_tai(Unit::Millisecond),
            epoch.to_unix_milliseconds(),
        );
        let epoch = Epoch::from_tai_seconds(0.0);
        eprintln!(
            "time:{epoch:?} scale:{} tai:{} utc:{}",
            epoch.time_scale,
            epoch.to_tai(Unit::Millisecond),
            epoch.to_unix_milliseconds(),
        );
    }
}
