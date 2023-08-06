// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Compaction picker.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};

use common_types::time::Timestamp;
use log::{debug, info};
use macros::define_result;
use snafu::Snafu;
use time_ext::TimeUnit;

use crate::{
    compaction::{
        CompactionInputFiles, CompactionStrategy, CompactionTask, CompactionTaskBuilder,
        SizeTieredCompactionOptions, TimeWindowCompactionOptions,
    },
    sst::{
        file::{FileHandle, Level},
        manager::LevelsController,
    },
};

#[derive(Debug, Snafu)]
pub enum Error {}

define_result!(Error);

#[derive(Clone)]
pub struct PickerContext {
    pub segment_duration: Duration,
    /// The ttl of the data in sst.
    pub ttl: Option<Duration>,
    pub strategy: CompactionStrategy,
}

impl PickerContext {
    fn size_tiered_opts(&self) -> SizeTieredCompactionOptions {
        match self.strategy {
            CompactionStrategy::SizeTiered(opts) => opts,
            _ => SizeTieredCompactionOptions::default(),
        }
    }

    fn time_window_opts(&self) -> TimeWindowCompactionOptions {
        match self.strategy {
            CompactionStrategy::TimeWindow(opts) => opts,
            _ => TimeWindowCompactionOptions::default(),
        }
    }
}

pub trait CompactionPicker {
    /// Pick candidate files for compaction.
    ///
    /// Note: files being compacted should be ignored.
    fn pick_compaction(
        &self,
        ctx: PickerContext,
        levels_controller: &mut LevelsController,
    ) -> Result<CompactionTask>;
}

pub type CompactionPickerRef = Arc<dyn CompactionPicker + Send + Sync>;

trait LevelPicker {
    /// Pick candidate files for compaction at level
    fn pick_candidates_at_level(
        &self,
        ctx: &PickerContext,
        levels_controller: &LevelsController,
        level: Level,
        expire_time: Option<Timestamp>,
    ) -> Option<Vec<FileHandle>>;
}

type LevelPickerRef = Arc<dyn LevelPicker + Send + Sync>;

pub struct CommonCompactionPicker {
    level_picker: LevelPickerRef,
}

impl CommonCompactionPicker {
    pub fn new(strategy: CompactionStrategy) -> Self {
        let level_picker: LevelPickerRef = match strategy {
            CompactionStrategy::SizeTiered(_) => Arc::new(SizeTieredPicker::default()),
            CompactionStrategy::TimeWindow(_) | CompactionStrategy::Default => {
                Arc::new(TimeWindowPicker::default())
            }
        };
        Self { level_picker }
    }

    fn pick_compact_candidates(
        &self,
        ctx: &PickerContext,
        levels_controller: &LevelsController,
        expire_time: Option<Timestamp>,
    ) -> Option<CompactionInputFiles> {
        for level in levels_controller.levels() {
            if let Some(files) = self.level_picker.pick_candidates_at_level(
                ctx,
                levels_controller,
                level,
                expire_time,
            ) {
                return Some(CompactionInputFiles {
                    level,
                    files,
                    output_level: level.next(),
                });
            }
        }

        None
    }
}

impl CompactionPicker for CommonCompactionPicker {
    fn pick_compaction(
        &self,
        ctx: PickerContext,
        levels_controller: &mut LevelsController,
    ) -> Result<CompactionTask> {
        let expire_time = ctx.ttl.map(Timestamp::expire_time);
        let mut builder =
            CompactionTaskBuilder::with_expired(levels_controller.expired_ssts(expire_time));

        if let Some(input_files) =
            self.pick_compact_candidates(&ctx, levels_controller, expire_time)
        {
            info!(
                "Compaction strategy: {:?} picker pick files to compact, input_files:{:?}",
                ctx.strategy, input_files
            );

            builder.add_inputs(input_files);
        }

        Ok(builder.build())
    }
}

#[inline]
fn find_uncompact_files(
    levels_controller: &LevelsController,
    level: Level,
    expire_time: Option<Timestamp>,
) -> Vec<FileHandle> {
    levels_controller
        .iter_ssts_at_level(level)
        // Only use files not being compacted and not expired.
        .filter(|file| !file.being_compacted() && !file.time_range().is_expired(expire_time))
        .cloned()
        .collect()
}

// Trim the largest sstables off the end to meet the `max_threshold` and
// `max_input_sstable_size`
fn trim_to_threshold(
    input_files: Vec<FileHandle>,
    max_threshold: usize,
    max_input_sstable_size: u64,
) -> Vec<FileHandle> {
    let mut input_size = 0;
    input_files
        .into_iter()
        .take(max_threshold)
        .take_while(|f| {
            input_size += f.size();
            input_size <= max_input_sstable_size
        })
        .collect()
}

// TODO: Remove this function when pick_by_seq is stable.
fn prefer_pick_by_seq() -> bool {
    std::env::var("CERESDB_COMPACT_PICK_BY_SEQ").unwrap_or_else(|_| "true".to_string()) == "true"
}

/// Size tiered compaction strategy
///
/// Origin solution[1] will only consider file size, but this will cause data
/// corrupt, see https://github.com/CeresDB/ceresdb/pull/1041
///
/// So we could only compact files with adjacent seq, or ssts without
/// overlapping key range among them. Currently solution is relative simple,
/// only pick adjacent sst. Maybe a better, but more complex solution could be
/// introduced later.
///
/// [1]: https://github.com/jeffjirsa/twcs/blob/master/src/main/java/com/jeffjirsa/cassandra/db/compaction/SizeTieredCompactionStrategy.java
pub struct SizeTieredPicker {
    pick_by_seq: bool,
}

impl Default for SizeTieredPicker {
    fn default() -> Self {
        Self {
            pick_by_seq: prefer_pick_by_seq(),
        }
    }
}

/// Similar size files group
#[derive(Debug, Clone)]
struct Bucket {
    pub avg_size: usize,
    pub files: Vec<FileHandle>,
}

impl Bucket {
    fn with_file(file: &FileHandle) -> Self {
        Self {
            avg_size: file.size() as usize,
            files: vec![file.clone()],
        }
    }

    fn with_files(files: Vec<FileHandle>) -> Self {
        let total: usize = files.iter().map(|f| f.size() as usize).sum();
        let avg_size = if files.is_empty() {
            0
        } else {
            total / files.len()
        };
        Self { avg_size, files }
    }

    fn insert_file(&mut self, file: &FileHandle) {
        let total_size = self.files.len() * self.avg_size + file.size() as usize;
        self.avg_size = total_size / (self.files.len() + 1);
        self.files.push(file.clone());
    }

    fn get_hotness_map(&self) -> HashMap<FileHandle, f64> {
        self.files
            .iter()
            .map(|f| (f.clone(), Self::hotness(f)))
            .collect()
    }

    #[inline]
    fn hotness(f: &FileHandle) -> f64 {
        //prevent NAN hotness
        let row_num = f.row_num().max(1);
        f.read_meter().h2_rate() / (row_num as f64)
    }
}

impl LevelPicker for SizeTieredPicker {
    fn pick_candidates_at_level(
        &self,
        ctx: &PickerContext,
        levels_controller: &LevelsController,
        level: Level,
        expire_time: Option<Timestamp>,
    ) -> Option<Vec<FileHandle>> {
        let files_by_segment =
            Self::files_by_segment(levels_controller, level, ctx.segment_duration, expire_time);
        if files_by_segment.is_empty() {
            return None;
        }

        let opts = ctx.size_tiered_opts();
        // Iterate the segment in reverse order, so newest segment is examined first.
        for (idx, (segment_key, segment)) in files_by_segment.iter().rev().enumerate() {
            let files = self.pick_ssts(segment.to_vec(), &opts);
            if files.is_some() {
                info!("Compact segment, idx:{idx}, segment_key:{segment_key:?}, files:{segment:?}");
                return files;
            }
            debug!("No compaction necessary for segment, idx:{idx}, segment_key:{segment_key:?}");
        }

        None
    }
}

impl SizeTieredPicker {
    fn pick_ssts(
        &self,
        files: Vec<FileHandle>,
        opts: &SizeTieredCompactionOptions,
    ) -> Option<Vec<FileHandle>> {
        if self.pick_by_seq {
            return Self::pick_by_seq(
                files,
                opts.min_threshold,
                opts.max_threshold,
                opts.max_input_sstable_size.as_byte(),
            );
        }

        Self::pick_by_size(files, opts)
    }

    fn pick_by_seq(
        mut files: Vec<FileHandle>,
        min_threshold: usize,
        max_threshold: usize,
        max_input_sstable_size: u64,
    ) -> Option<Vec<FileHandle>> {
        // Sort files by max_seq desc.
        files.sort_unstable_by_key(|b| std::cmp::Reverse(b.max_sequence()));

        'outer: for start in 0..files.len() {
            // Try max_threshold first, since we hope to compact as many small files as we
            // can.
            for step in (min_threshold..=max_threshold).rev() {
                let end = (start + step).min(files.len());
                if end - start < min_threshold {
                    // too little files, switch to next loop and find again.
                    continue 'outer;
                }

                let curr_size: u64 = files[start..end].iter().map(|f| f.size()).sum();
                if curr_size <= max_input_sstable_size {
                    return Some(files[start..end].to_vec());
                }
            }
        }

        None
    }

    fn pick_by_size(
        files: Vec<FileHandle>,
        opts: &SizeTieredCompactionOptions,
    ) -> Option<Vec<FileHandle>> {
        let buckets = Self::get_buckets(
            files,
            opts.bucket_high,
            opts.bucket_low,
            opts.min_sstable_size.as_byte() as f32,
        );

        Self::most_interesting_bucket(
            buckets,
            opts.min_threshold,
            opts.max_threshold,
            opts.max_input_sstable_size.as_byte(),
        )
    }

    ///  Group files of similar size into buckets.
    fn get_buckets(
        mut files: Vec<FileHandle>,
        bucket_high: f32,
        bucket_low: f32,
        min_sst_size: f32,
    ) -> Vec<Bucket> {
        // sort by file length
        files.sort_unstable_by_key(FileHandle::size);

        let mut buckets: Vec<Bucket> = Vec::new();
        'outer: for sst in &files {
            let size = sst.size() as f32;
            // look for a bucket containing similar-sized files:
            // group in the same bucket if it's w/in 50% of the average for this bucket,
            // or this file and the bucket are all considered "small" (less than
            // `min_sst_size`)
            for bucket in buckets.iter_mut() {
                let old_avg_size = bucket.avg_size as f32;
                if (size > (old_avg_size * bucket_low) && size < (old_avg_size * bucket_high))
                    || (size < min_sst_size && old_avg_size < min_sst_size)
                {
                    // find a similar file, insert it into bucket
                    bucket.insert_file(sst);
                    continue 'outer;
                }
            }

            // no similar bucket found
            // put it in a new bucket
            buckets.push(Bucket::with_file(sst));
        }

        debug!("Group files of similar size into buckets: {:?}", buckets);

        buckets
    }

    fn most_interesting_bucket(
        buckets: Vec<Bucket>,
        min_threshold: usize,
        max_threshold: usize,
        max_input_sstable_size: u64,
    ) -> Option<Vec<FileHandle>> {
        debug!(
            "Find most_interesting_bucket buckets:{:?}, min:{}, max:{}",
            buckets, min_threshold, max_threshold
        );

        let mut pruned_bucket_and_hotness = Vec::with_capacity(buckets.len());
        // skip buckets containing less than min_threshold sstables,
        // and limit other buckets to max_threshold sstables
        for bucket in buckets {
            let (bucket, hotness) =
                Self::trim_to_threshold_with_hotness(bucket, max_threshold, max_input_sstable_size);
            if bucket.files.len() >= min_threshold {
                pruned_bucket_and_hotness.push((bucket, hotness));
            }
        }

        if pruned_bucket_and_hotness.is_empty() {
            return None;
        }

        // Find the hottest bucket
        if let Some((bucket, hotness)) =
            pruned_bucket_and_hotness
                .into_iter()
                .max_by(|(b1, h1), (b2, h2)| {
                    let c = h1.partial_cmp(h2).unwrap();
                    if !c.is_eq() {
                        return c;
                    }
                    // TODO(boyan), compacting smallest sstables first?
                    b1.avg_size.cmp(&b2.avg_size)
                })
        {
            debug!(
                "Find the hottest bucket, hotness: {}, bucket: {:?}",
                hotness, bucket
            );
            Some(bucket.files)
        } else {
            None
        }
    }

    fn files_by_segment(
        levels_controller: &LevelsController,
        level: Level,
        segment_duration: Duration,
        expire_time: Option<Timestamp>,
    ) -> BTreeMap<Timestamp, Vec<FileHandle>> {
        let mut files_by_segment = BTreeMap::new();
        let uncompact_files = find_uncompact_files(levels_controller, level, expire_time);
        for file in uncompact_files {
            // We use the end time of the range to calculate segment.
            let segment = file
                .time_range()
                .exclusive_end()
                .truncate_by(segment_duration);
            let files = files_by_segment.entry(segment).or_insert_with(Vec::new);
            files.push(file);
        }

        files_by_segment
    }

    fn trim_to_threshold_with_hotness(
        bucket: Bucket,
        max_threshold: usize,
        max_input_sstable_size: u64,
    ) -> (Bucket, f64) {
        let hotness_snapshot = bucket.get_hotness_map();

        // Sort by sstable hotness (descending).
        let mut sorted_files = bucket.files.to_vec();
        sorted_files.sort_unstable_by(|f1, f2| {
            hotness_snapshot[f1]
                .partial_cmp(&hotness_snapshot[f2])
                .unwrap()
                .reverse()
        });

        let pruned_bucket = trim_to_threshold(sorted_files, max_threshold, max_input_sstable_size);
        // bucket hotness is the sum of the hotness of all sstable members
        let bucket_hotness = pruned_bucket.iter().map(Bucket::hotness).sum();

        (Bucket::with_files(pruned_bucket), bucket_hotness)
    }
}

/// Time window compaction strategy
/// See https://github.com/jeffjirsa/twcs/blob/master/src/main/java/com/jeffjirsa/cassandra/db/compaction/TimeWindowCompactionStrategy.java
pub struct TimeWindowPicker {
    pick_by_seq: bool,
}

impl Default for TimeWindowPicker {
    fn default() -> Self {
        Self {
            pick_by_seq: prefer_pick_by_seq(),
        }
    }
}

impl TimeWindowPicker {
    fn get_window_bounds_in_millis(window: &Duration, ts: i64) -> (i64, i64) {
        let ts_secs = ts / 1000;

        let size = window.as_secs() as i64;

        let lower = ts_secs - (ts_secs % size);
        let upper = lower + size - 1;

        (lower * 1000, upper * 1000)
    }

    #[inline]
    fn resolve_timestamp(ts: i64, timestamp_resolution: TimeUnit) -> i64 {
        match timestamp_resolution {
            TimeUnit::Microseconds => ts / 1000,
            TimeUnit::Nanoseconds => ts / 1000000,
            TimeUnit::Seconds => ts * 1000,
            TimeUnit::Milliseconds => ts,
            // the option is validated before, so it won't reach here
            _ => unreachable!(),
        }
    }

    ///  Group files of similar timestamp into buckets.
    fn get_buckets(
        files: &[FileHandle],
        window: &Duration,
        timestamp_resolution: TimeUnit,
    ) -> (HashMap<i64, Vec<FileHandle>>, i64) {
        let mut max_ts = 0i64;
        let mut buckets: HashMap<i64, Vec<FileHandle>> = HashMap::new();
        for f in files {
            let ts = f.time_range_ref().exclusive_end().as_i64();

            let ts = Self::resolve_timestamp(ts, timestamp_resolution);

            let (left, _) = Self::get_window_bounds_in_millis(window, ts);

            let bucket_files = buckets.entry(left).or_insert_with(Vec::new);

            bucket_files.push(f.clone());

            if left > max_ts {
                max_ts = left;
            }
        }

        debug!(
            "Group files of similar timestamp into buckets: {:?}",
            buckets
        );
        (buckets, max_ts)
    }

    fn newest_bucket(
        &self,
        buckets: HashMap<i64, Vec<FileHandle>>,
        size_tiered_opts: SizeTieredCompactionOptions,
        now: i64,
    ) -> Option<Vec<FileHandle>> {
        // If the current bucket has at least minThreshold SSTables, choose that one.
        // For any other bucket, at least 2 SSTables is enough.
        // In any case, limit to max_threshold SSTables.

        let all_keys: BTreeSet<_> = buckets.keys().collect();

        // First compact latest buckets
        for key in all_keys.into_iter().rev() {
            if let Some(bucket) = buckets.get(key) {
                debug!("Newest bucket loop, key:{key}, now:{now}");

                if bucket.len() >= size_tiered_opts.min_threshold && *key >= now {
                    // If we're in the newest bucket, we'll use STCS to prioritize sstables
                    let size_picker = SizeTieredPicker::default();
                    let files = size_picker.pick_ssts(bucket.to_vec(), &size_tiered_opts);

                    if files.is_some() {
                        return files;
                    }
                } else if bucket.len() >= 2 && *key < now {
                    debug!("Bucket size {} >= 2 and not in current bucket, compacting what's here: {:?}", bucket.len(), bucket);
                    let files = self.pick_sst_for_old_bucket(bucket.to_vec(), &size_tiered_opts);
                    if files.is_some() {
                        return files;
                    }
                } else {
                    debug!(
                        "No compaction necessary for bucket size {} , key {}, now {}",
                        bucket.len(),
                        key,
                        now
                    );
                }
            }
        }

        None
    }

    fn pick_sst_for_old_bucket(
        &self,
        mut files: Vec<FileHandle>,
        size_tiered_opts: &SizeTieredCompactionOptions,
    ) -> Option<Vec<FileHandle>> {
        let max_input_size = size_tiered_opts.max_input_sstable_size.as_byte();
        // For old bucket, sst is likely already compacted, so min_thresold is not very
        // strict, and greedy as `size_tiered_opts`.
        let min_threshold = 2;
        if self.pick_by_seq {
            return SizeTieredPicker::pick_by_seq(
                files,
                min_threshold,
                size_tiered_opts.max_threshold,
                max_input_size,
            );
        }

        files.sort_unstable_by_key(FileHandle::size);
        let candidate_files =
            trim_to_threshold(files, size_tiered_opts.max_threshold, max_input_size);
        if candidate_files.len() >= min_threshold {
            return Some(candidate_files);
        }

        None
    }

    /// Get current window timestamp, the caller MUST ensure the level has ssts,
    /// panic otherwise.
    fn get_current_window(
        levels_controller: &LevelsController,
        level: Level,
        window: &Duration,
        timestamp_resolution: TimeUnit,
    ) -> i64 {
        // always find the latest sst here
        let now = levels_controller
            .latest_sst(level)
            .unwrap()
            .time_range()
            .exclusive_end()
            .as_i64();
        let now = Self::resolve_timestamp(now, timestamp_resolution);
        Self::get_window_bounds_in_millis(window, now).0
    }
}

impl LevelPicker for TimeWindowPicker {
    fn pick_candidates_at_level(
        &self,
        ctx: &PickerContext,
        levels_controller: &LevelsController,
        level: Level,
        expire_time: Option<Timestamp>,
    ) -> Option<Vec<FileHandle>> {
        let uncompact_files = find_uncompact_files(levels_controller, level, expire_time);

        if uncompact_files.is_empty() {
            return None;
        }

        let opts = ctx.time_window_opts();

        debug!("TWCS compaction options: {:?}", opts);

        let (buckets, max_bucket_ts) = Self::get_buckets(
            &uncompact_files,
            &ctx.segment_duration,
            opts.timestamp_resolution,
        );

        let now = Self::get_current_window(
            levels_controller,
            level,
            &ctx.segment_duration,
            opts.timestamp_resolution,
        );
        debug!(
            "TWCS current window is {}, max_bucket_ts: {}",
            now, max_bucket_ts
        );
        assert!(now >= max_bucket_ts);

        self.newest_bucket(buckets, opts.size_tiered, now)
    }
}