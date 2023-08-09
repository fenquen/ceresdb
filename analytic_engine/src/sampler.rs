// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Segment duration sampler.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use common_types::time::{TimeRange, Timestamp};
use macros::define_result;
use snafu::{ensure, Backtrace, Snafu};

use crate::table_options;

/// Initial size of timestamps set.
const INIT_CAPACITY: usize = 1000;
const HOUR_MS: u64 = 3600 * 1000;
const DAY_MS: u64 = 24 * HOUR_MS;
const AVAILABLE_DURATIONS: [u64; 8] = [
    2 * HOUR_MS,
    DAY_MS,
    7 * DAY_MS,
    30 * DAY_MS,
    180 * DAY_MS,
    360 * DAY_MS,
    5 * 360 * DAY_MS,
    10 * 360 * DAY_MS,
];
const INTERVAL_RATIO: f64 = 0.9;
/// Expected points per timeseries in a segment, used to pick a proper segment
/// duration.
const POINTS_PER_SERIES: u64 = 100;
/// Max timestamp that wont overflow even using max duration.
const MAX_TIMESTAMP_MS_FOR_DURATION: i64 =
    i64::MAX - 2 * AVAILABLE_DURATIONS[AVAILABLE_DURATIONS.len() - 1] as i64;
/// Minimun sample timestamps to compute duration.
const MIN_SAMPLES: usize = 2;

#[derive(Debug, Snafu)]
#[snafu(display(
    "Invalid timestamp to collect, timestamp:{:?}.\nBacktrace:\n{}",
    timestamp,
    backtrace
))]
pub struct Error {
    timestamp: Timestamp,
    backtrace: Backtrace,
}

define_result!(Error);

/// Segment duration sampler.
///
/// Collects all timestamps and then yield a suggested segment duration to hold
/// all data with similar timestamp interval.
pub trait DurationSampler {
    /// Collect a timestamp.
    fn collect(&self, timestamp: Timestamp) -> Result<()>;

    /// Returns a suggested duration to partition the timestamps or default
    /// duration if no enough timestamp has been sampled.
    ///
    /// Note that this method may be invoked more than once.
    fn suggest_duration(&self) -> Duration;

    /// Returns a vector of time range with suggested duration that can hold all
    /// timestamps collected by this sampler.
    fn ranges(&self) -> Vec<TimeRange>;

    // TODO(yingwen): Memory usage.
}

pub type SamplerRef = Arc<dyn DurationSampler + Send + Sync>;

struct State {
    /// Deduplicated timestamps.
    deduped_timestamps: HashSet<Timestamp>,
    /// Cached suggested duration.
    duration: Option<Duration>,
    /// Sorted timestamps cache, empty if `duration` is None.
    sorted_timestamps: Vec<Timestamp>,
}

impl State {
    fn clear_cache(&mut self) {
        self.duration = None;
        self.sorted_timestamps.clear();
    }
}

pub struct DefaultSampler {
    state: Mutex<State>,
}

impl Default for DefaultSampler {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                deduped_timestamps: HashSet::with_capacity(INIT_CAPACITY),
                duration: None,
                sorted_timestamps: Vec::new(),
            }),
        }
    }
}

impl DurationSampler for DefaultSampler {
    fn collect(&self, timestamp: Timestamp) -> Result<()> {
        ensure!(
            timestamp.as_i64() < MAX_TIMESTAMP_MS_FOR_DURATION,
            Context { timestamp }
        );

        let mut state = self.state.lock().unwrap();
        state.deduped_timestamps.insert(timestamp);
        state.clear_cache();

        Ok(())
    }

    fn suggest_duration(&self) -> Duration {
        if let Some(v) = self.duration() {
            return v;
        }

        let timestamps = self.compute_sorted_timestamps();
        let picked = match evaluate_interval(&timestamps) {
            Some(interval) => pick_duration(interval),
            None => table_options::DEFAULT_SEGMENT_DURATION,
        };

        {
            // Cache the picked duration.
            let mut state = self.state.lock().unwrap();
            state.duration = Some(picked);
            state.sorted_timestamps = timestamps;
        }

        picked
    }

    fn ranges(&self) -> Vec<TimeRange> {
        let duration = self.suggest_duration();
        let sorted_timestamps = self.cached_sorted_timestamps();
        // This type hint is needed to make `ranges.last()` work.
        let mut ranges: Vec<TimeRange> = Vec::new();

        for ts in sorted_timestamps {
            if let Some(range) = ranges.last() {
                if range.contains(ts) {
                    continue;
                }
            }

            // collect() ensures timestamp won't overflow.
            let range = TimeRange::bucket_of(ts, duration).unwrap();
            ranges.push(range);
        }

        ranges
    }
}

impl DefaultSampler {
    fn cached_sorted_timestamps(&self) -> Vec<Timestamp> {
        self.state.lock().unwrap().sorted_timestamps.clone()
    }

    fn compute_sorted_timestamps(&self) -> Vec<Timestamp> {
        let mut timestamps: Vec<_> = {
            let state = self.state.lock().unwrap();
            state.deduped_timestamps.iter().copied().collect()
        };

        timestamps.sort_unstable();

        timestamps
    }

    fn duration(&self) -> Option<Duration> {
        self.state.lock().unwrap().duration
    }
}

fn evaluate_interval(sorted_timestamps: &[Timestamp]) -> Option<u64> {
    if sorted_timestamps.len() < MIN_SAMPLES {
        return None;
    }

    let mut intervals = Vec::with_capacity(sorted_timestamps.len());
    for i in 0..sorted_timestamps.len() - 1 {
        let current = sorted_timestamps[i];
        let next = sorted_timestamps[i + 1];
        let interval = next.as_i64() - current.as_i64();
        intervals.push(interval);
    }

    intervals.sort_unstable();

    let mut index = (intervals.len() as f64 * INTERVAL_RATIO) as usize;
    if index > 1 {
        index -= 1;
    };
    let selected = intervals[index];
    // Interval should larger than 0.
    assert!(selected > 0);

    Some(selected as u64)
}

fn pick_duration(interval: u64) -> Duration {
    let scaled_interval = interval.checked_mul(POINTS_PER_SERIES).unwrap_or(u64::MAX);
    for du_ms in AVAILABLE_DURATIONS {
        if du_ms > scaled_interval {
            return Duration::from_millis(du_ms);
        }
    }

    // No duration larger than scaled interval, returns the largest duration.
    Duration::from_millis(AVAILABLE_DURATIONS[AVAILABLE_DURATIONS.len() - 1])
}