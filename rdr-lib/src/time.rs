use std::ops::Deref;
use std::str::FromStr;

use hifitime::efmt::{Format, Formatter};
use hifitime::Epoch;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
    // Difference betweeh hifitime epoch and JPSS epoch (Jan 1, 1958) in seconds
    const IET_DELTA: f64 = 1_830_297_600.0;

    pub fn now() -> Self {
        Time(Epoch::now().expect("failed to get system time"))
    }

    pub fn from_epoch(epoch: Epoch) -> Self {
        Time(epoch)
    }

    pub fn from_utc(micros: f64) -> Self {
        Self(Epoch::from_unix_seconds(micros / 1_000_000.0))
    }

    pub fn from_iet(micros: f64) -> Self {
        Self(Epoch::from_tai_seconds(
            micros / 1_000_000.0 + Self::IET_DELTA,
        ))
    }

    /// Return UTC microseconds since Jan 1, 1970
    pub fn utc(&self) -> f64 {
        self.0.to_unix_milliseconds() * 1000.0
    }
    /// Return TAI microseconds since Jan 1, 1958
    pub fn iet(&self) -> f64 {
        self.0.to_tai(hifitime::Unit::Microsecond) - (Self::IET_DELTA * 1_000_000.0)
    }

    /// Format ourself using the provided format string.
    ///
    /// See [hifitime::efmt::Format].
    pub fn format(&self, fmt: &str) -> String {
        let fmt = Format::from_str(fmt).unwrap();
        let formatter = Formatter::new(self.0, fmt);
        format!("{formatter}")
    }

    pub fn truncate(&self, period_micros: u64) -> Self {
        let period_micros = period_micros as f64;
        let t = (self.iet() % period_micros) * period_micros / 1_000_000.0;
        Time(Epoch::from_tai_seconds(t + Self::IET_DELTA))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn format() {
        let time = Time(Epoch::from_unix_seconds(0.0));

        assert_eq!(
            time.format("%Y-%m-%dT%H:%M:%S%z"),
            "1970-01-01T00:00:00+00:00"
        );
    }

    #[test]
    fn utc() {
        let time = Time(Epoch::from_unix_seconds(0.0));

        assert_eq!(time.utc(), 0.0);
    }

    #[test]
    fn iet() {
        let time = Time(Epoch::from_unix_seconds(0.0));

        assert_eq!(time.iet(), 378_691_200_000_000.0);
    }
}
