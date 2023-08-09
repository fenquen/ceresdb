// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Time utilities

// TODO(yingwen): Move to common_types ?

use std::{
    convert::TryInto,
    fmt::{self, Write},
    ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Sub, SubAssign},
    str::FromStr,
    time::{Duration, Instant, UNIX_EPOCH},
};
use std::time::SystemTime;

use ceresdbproto::manifest as manifest_pb;
use chrono::{DateTime, Utc};
use common_types::time::Timestamp;
use macros::define_result;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use snafu::{Backtrace, GenerateBacktrace, Snafu};

#[derive(Debug, Snafu)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("Failed to parse duration, err:{}.\nBacktrace:\n{}", err, backtrace))]
    ParseDuration { err: String, backtrace: Backtrace },
}

define_result!(Error);

const TIME_MAGNITUDE_1: u64 = 1000;
const TIME_MAGNITUDE_2: u64 = 60;
const TIME_MAGNITUDE_3: u64 = 24;
const UNIT: u64 = 1;
const MS: u64 = UNIT;
const SECOND: u64 = MS * TIME_MAGNITUDE_1;
const MINUTE: u64 = SECOND * TIME_MAGNITUDE_2;
const HOUR: u64 = MINUTE * TIME_MAGNITUDE_2;
const DAY: u64 = HOUR * TIME_MAGNITUDE_3;

/// Convert Duration to milliseconds.
///
/// Panic if overflow. Mainly used by `ReadableDuration`.
#[inline]
fn duration_to_ms(d: Duration) -> u64 {
    let nanos = u64::from(d.subsec_nanos());
    // Most of case, we can't have so large Duration, so here just panic if overflow
    // now.
    d.as_secs() * 1_000 + (nanos / 1_000_000)
}

#[derive(Clone, Debug, Copy, PartialEq, Eq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeUnit {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
    Minutes,
    Hours,
    Days,
}

impl From<TimeUnit> for manifest_pb::TimeUnit {
    fn from(unit: TimeUnit) -> Self {
        match unit {
            TimeUnit::Nanoseconds => manifest_pb::TimeUnit::Nanoseconds,
            TimeUnit::Microseconds => manifest_pb::TimeUnit::Microseconds,
            TimeUnit::Milliseconds => manifest_pb::TimeUnit::Milliseconds,
            TimeUnit::Seconds => manifest_pb::TimeUnit::Seconds,
            TimeUnit::Minutes => manifest_pb::TimeUnit::Minutes,
            TimeUnit::Hours => manifest_pb::TimeUnit::Hours,
            TimeUnit::Days => manifest_pb::TimeUnit::Days,
        }
    }
}

impl From<manifest_pb::TimeUnit> for TimeUnit {
    fn from(unit: manifest_pb::TimeUnit) -> Self {
        match unit {
            manifest_pb::TimeUnit::Nanoseconds => TimeUnit::Nanoseconds,
            manifest_pb::TimeUnit::Microseconds => TimeUnit::Microseconds,
            manifest_pb::TimeUnit::Milliseconds => TimeUnit::Milliseconds,
            manifest_pb::TimeUnit::Seconds => TimeUnit::Seconds,
            manifest_pb::TimeUnit::Minutes => TimeUnit::Minutes,
            manifest_pb::TimeUnit::Hours => TimeUnit::Hours,
            manifest_pb::TimeUnit::Days => TimeUnit::Days,
        }
    }
}

impl FromStr for TimeUnit {
    type Err = String;

    fn from_str(tu_str: &str) -> std::result::Result<TimeUnit, String> {
        let tu_str = tu_str.trim();
        if !tu_str.is_ascii() {
            return Err(format!("unexpected ascii string: {tu_str}"));
        }

        match tu_str.to_lowercase().as_str() {
            "nanoseconds" => Ok(TimeUnit::Nanoseconds),
            "microseconds" => Ok(TimeUnit::Microseconds),
            "milliseconds" => Ok(TimeUnit::Milliseconds),
            "seconds" => Ok(TimeUnit::Seconds),
            "minutes" => Ok(TimeUnit::Minutes),
            "hours" => Ok(TimeUnit::Hours),
            "days" => Ok(TimeUnit::Days),
            _ => Err(format!("unexpected TimeUnit: {tu_str}")),
        }
    }
}

impl fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            TimeUnit::Nanoseconds => "nanoseconds",
            TimeUnit::Microseconds => "microseconds",
            TimeUnit::Milliseconds => "milliseconds",
            TimeUnit::Seconds => "seconds",
            TimeUnit::Minutes => "minutes",
            TimeUnit::Hours => "hours",
            TimeUnit::Days => "days",
        };
        write!(f, "{s}")
    }
}

pub trait DurationExt {
    /// Convert into u64.
    ///
    /// Returns u64::MAX if overflow
    fn as_millis_u64(&self) -> u64;
}

impl DurationExt for Duration {
    #[inline]
    fn as_millis_u64(&self) -> u64 {
        match self.as_millis().try_into() {
            Ok(v) => v,
            Err(_) => u64::MAX,
        }
    }
}

pub trait InstantExt {
    fn saturating_elapsed(&self) -> Duration;

    /// Check whether this instant is reached
    fn check_deadline(&self) -> bool;
}

impl InstantExt for Instant {
    fn saturating_elapsed(&self) -> Duration {
        Instant::now().saturating_duration_since(*self)
    }

    fn check_deadline(&self) -> bool {
        self.saturating_elapsed().is_zero()
    }
}

#[inline]
pub fn secs_to_nanos(s: u64) -> u64 {
    s * 1_000_000_000
}

#[inline]
pub fn current_time_millis() -> u64 {
    Utc::now().timestamp_millis() as u64
}

#[inline]
pub fn current_as_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

#[inline]
pub fn format_as_ymdhms(unix_timestamp: i64) -> String {
    let dt = DateTime::<Utc>::from(UNIX_EPOCH + Duration::from_millis(unix_timestamp as u64));
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub fn try_to_millis(ts: i64) -> Option<Timestamp> {
    // https://help.aliyun.com/document_detail/60683.html
    if (4294968..=4294967295).contains(&ts) {
        return Some(Timestamp::new(ts * 1000));
    }
    if (4294967296..=9999999999999).contains(&ts) {
        return Some(Timestamp::new(ts));
    }
    None
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Default)]
pub struct ReadableDuration(pub Duration);

impl Add for ReadableDuration {
    type Output = ReadableDuration;

    fn add(self, rhs: ReadableDuration) -> ReadableDuration {
        Self(self.0 + rhs.0)
    }
}

impl AddAssign for ReadableDuration {
    fn add_assign(&mut self, rhs: ReadableDuration) {
        *self = *self + rhs;
    }
}

impl Sub for ReadableDuration {
    type Output = ReadableDuration;

    fn sub(self, rhs: ReadableDuration) -> ReadableDuration {
        Self(self.0 - rhs.0)
    }
}

impl SubAssign for ReadableDuration {
    fn sub_assign(&mut self, rhs: ReadableDuration) {
        *self = *self - rhs;
    }
}

impl Mul<u32> for ReadableDuration {
    type Output = ReadableDuration;

    fn mul(self, rhs: u32) -> Self::Output {
        Self(self.0 * rhs)
    }
}

impl MulAssign<u32> for ReadableDuration {
    fn mul_assign(&mut self, rhs: u32) {
        *self = *self * rhs;
    }
}

impl Div<u32> for ReadableDuration {
    type Output = ReadableDuration;

    fn div(self, rhs: u32) -> ReadableDuration {
        Self(self.0 / rhs)
    }
}

impl DivAssign<u32> for ReadableDuration {
    fn div_assign(&mut self, rhs: u32) {
        *self = *self / rhs;
    }
}

impl From<ReadableDuration> for Duration {
    fn from(readable: ReadableDuration) -> Duration {
        readable.0
    }
}

// yingwen: Support From<Duration>.
impl From<Duration> for ReadableDuration {
    fn from(t: Duration) -> ReadableDuration {
        ReadableDuration(t)
    }
}

impl FromStr for ReadableDuration {
    type Err = String;

    fn from_str(dur_str: &str) -> std::result::Result<ReadableDuration, String> {
        let dur_str = dur_str.trim();
        if !dur_str.is_ascii() {
            return Err(format!("unexpected ascii string: {dur_str}"));
        }
        let err_msg = "valid duration, only d, h, m, s, ms are supported.".to_owned();
        let mut left = dur_str.as_bytes();
        let mut last_unit = DAY + 1;
        let mut dur = 0f64;
        while let Some(idx) = left.iter().position(|c| b"dhms".contains(c)) {
            let (first, second) = left.split_at(idx);
            let unit = if second.starts_with(b"ms") {
                left = &left[idx + 2..];
                MS
            } else {
                let u = match second[0] {
                    b'd' => DAY,
                    b'h' => HOUR,
                    b'm' => MINUTE,
                    b's' => SECOND,
                    _ => return Err(err_msg),
                };
                left = &left[idx + 1..];
                u
            };
            if unit >= last_unit {
                return Err("d, h, m, s, ms should occur in given order.".to_owned());
            }
            // do we need to check 12h360m?
            let number_str = unsafe { std::str::from_utf8_unchecked(first) };
            dur += match number_str.trim().parse::<f64>() {
                Ok(n) => n * unit as f64,
                Err(_) => return Err(err_msg),
            };
            last_unit = unit;
        }
        if !left.is_empty() {
            return Err(err_msg);
        }
        if dur.is_sign_negative() {
            return Err("duration should be positive.".to_owned());
        }
        let secs = dur as u64 / SECOND;
        let millis = (dur as u64 % SECOND) as u32 * 1_000_000;
        Ok(ReadableDuration(Duration::new(secs, millis)))
    }
}

impl ReadableDuration {
    pub const fn secs(secs: u64) -> ReadableDuration {
        ReadableDuration(Duration::from_secs(secs))
    }

    pub const fn millis(millis: u64) -> ReadableDuration {
        ReadableDuration(Duration::from_millis(millis))
    }

    pub const fn minutes(minutes: u64) -> ReadableDuration {
        ReadableDuration::secs(minutes * 60)
    }

    pub const fn hours(hours: u64) -> ReadableDuration {
        ReadableDuration::minutes(hours * 60)
    }

    pub const fn days(days: u64) -> ReadableDuration {
        ReadableDuration::hours(days * 24)
    }

    pub fn as_secs(&self) -> u64 {
        self.0.as_secs()
    }

    pub fn as_millis(&self) -> u64 {
        duration_to_ms(self.0)
    }

    pub fn is_zero(&self) -> bool {
        self.0.as_nanos() == 0
    }
}

impl fmt::Display for ReadableDuration {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut dur = duration_to_ms(self.0);
        let mut written = false;
        if dur >= DAY {
            written = true;
            write!(f, "{}d", dur / DAY)?;
            dur %= DAY;
        }
        if dur >= HOUR {
            written = true;
            write!(f, "{}h", dur / HOUR)?;
            dur %= HOUR;
        }
        if dur >= MINUTE {
            written = true;
            write!(f, "{}m", dur / MINUTE)?;
            dur %= MINUTE;
        }
        if dur >= SECOND {
            written = true;
            write!(f, "{}s", dur / SECOND)?;
            dur %= SECOND;
        }
        if dur > 0 {
            written = true;
            write!(f, "{dur}ms")?;
        }
        if !written {
            write!(f, "0s")?;
        }
        Ok(())
    }
}

impl Serialize for ReadableDuration {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut buffer = String::new();
        write!(buffer, "{self}").unwrap();
        serializer.serialize_str(&buffer)
    }
}

impl<'de> Deserialize<'de> for ReadableDuration {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DurVisitor;

        impl<'de> Visitor<'de> for DurVisitor {
            type Value = ReadableDuration;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("valid duration")
            }

            fn visit_str<E>(self, dur_str: &str) -> std::result::Result<ReadableDuration, E>
            where
                E: de::Error,
            {
                dur_str.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_str(DurVisitor)
    }
}

pub fn parse_duration(v: &str) -> Result<ReadableDuration> {
    v.parse::<ReadableDuration>()
        .map_err(|err| Error::ParseDuration {
            err,
            backtrace: Backtrace::generate(),
        })
}