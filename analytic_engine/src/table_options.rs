// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Constants for table options.

use std::{collections::HashMap, string::ToString, time::Duration};

use ceresdbproto::manifest as manifest_pb;
use common_types::{
    time::Timestamp, ARENA_BLOCK_SIZE, COMPACTION_STRATEGY, COMPRESSION, ENABLE_TTL,
    NUM_ROWS_PER_ROW_GROUP, OPTION_KEY_ENABLE_TTL, SEGMENT_DURATION, STORAGE_FORMAT, TTL,
    UPDATE_MODE, WRITE_BUFFER_SIZE,
};
use datafusion::parquet::basic::Compression as ParquetCompression;
use macros::define_result;
use serde::{Deserialize, Serialize};
use size_ext::ReadableSize;
use snafu::{Backtrace, GenerateBacktrace, OptionExt, ResultExt, Snafu};
use time_ext::{parse_duration, DurationExt, ReadableDuration, TimeUnit};

use crate::compaction::{
    self, CompactionStrategy, SizeTieredCompactionOptions, TimeWindowCompactionOptions,
};

const UPDATE_MODE_OVERWRITE: &str = "OVERWRITE";
const UPDATE_MODE_APPEND: &str = "APPEND";
const COMPRESSION_UNCOMPRESSED: &str = "UNCOMPRESSED";
const COMPRESSION_LZ4: &str = "LZ4";
const COMPRESSION_SNAPPY: &str = "SNAPPY";
const COMPRESSION_ZSTD: &str = "ZSTD";
const STORAGE_FORMAT_AUTO: &str = "AUTO";
const STORAGE_FORMAT_COLUMNAR: &str = "COLUMNAR";
const STORAGE_FORMAT_HYBRID: &str = "HYBRID";

/// Default bucket duration (1d)
const BUCKET_DURATION_1D: Duration = Duration::from_secs(24 * 60 * 60);
/// Default duration of a segment (2h).
pub const DEFAULT_SEGMENT_DURATION: Duration = Duration::from_secs(60 * 60 * 2);
/// Default arena block size (2M).
const DEFAULT_ARENA_BLOCK_SIZE: u32 = 2 * 1024 * 1024;
/// Default write buffer size (32M).
const DEFAULT_WRITE_BUFFER_SIZE: u32 = 32 * 1024 * 1024;
/// Default ttl of table (7d).
const DEFAULT_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// Default row number of a row group.
const DEFAULT_NUM_ROW_PER_ROW_GROUP: usize = 8192;

/// Max arena block size (2G)
const MAX_ARENA_BLOCK_SIZE: u32 = 2 * 1024 * 1024 * 1024;
/// Min arena block size (1K)
const MIN_ARENA_BLOCK_SIZE: u32 = 1024;
const MIN_NUM_ROWS_PER_ROW_GROUP: usize = 100;
const MAX_NUM_ROWS_PER_ROW_GROUP: usize = 10_000_000;

#[derive(Debug, Snafu)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("Failed to parse duration, err:{}.\nBacktrace:\n{}", source, backtrace))]
    ParseDuration {
        source: time_ext::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to parse size, err:{}.\nBacktrace:\n{}", err, backtrace))]
    ParseSize { err: String, backtrace: Backtrace },

    #[snafu(display("Failed to parse compaction strategy: {}, err: {}", value, source))]
    ParseStrategy {
        value: String,
        source: crate::compaction::Error,
    },
    #[snafu(display("Failed to parse int, err:{}.\nBacktrace:\n{}", source, backtrace))]
    ParseInt {
        source: std::num::ParseIntError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to parse bool, err:{}.\nBacktrace:\n{}", source, backtrace))]
    ParseBool {
        source: std::str::ParseBoolError,
        backtrace: Backtrace,
    },
    #[snafu(display("failed to parse update mode, raw str:{}.\nBacktrace:\n{}", s, backtrace))]
    ParseUpdateMode { s: String, backtrace: Backtrace },

    #[snafu(display("failed to parse compression, name:{}.\nBacktrace:\n{}", name, backtrace))]
    ParseCompressionName { name: String, backtrace: Backtrace },

    #[snafu(display("unknown storage format. value:{:?}.\nBacktrace:\n{}", value, backtrace))]
    UnknownStorageFormat { value: String, backtrace: Backtrace },

    #[snafu(display("unknown storage format. value:{:?}.\nBacktrace:\n{}", value, backtrace))]
    UnknownStorageFormatType { value: i32, backtrace: Backtrace },

    #[snafu(display("unknown storage format hint. value:{:?}.\nBacktrace:\n{}", value, backtrace))]
    UnknownStorageFormatHint { value: String, backtrace: Backtrace },

    #[snafu(display("storage format hint is missing.\nBacktrace:\n{}", backtrace))]
    MissingStorageFormatHint { backtrace: Backtrace },
}

define_result!(Error);

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
pub enum UpdateMode {
    Overwrite,
    Append,
}

impl UpdateMode {
    pub fn parse_from(s: &str) -> Result<Self> {
        if s.eq_ignore_ascii_case(UPDATE_MODE_OVERWRITE) {
            Ok(UpdateMode::Overwrite)
        } else if s.eq_ignore_ascii_case(UPDATE_MODE_APPEND) {
            Ok(UpdateMode::Append)
        } else {
            ParseUpdateMode { s }.fail()
        }
    }
}

impl ToString for UpdateMode {
    fn to_string(&self) -> String {
        match self {
            UpdateMode::Append => UPDATE_MODE_APPEND.to_string(),
            UpdateMode::Overwrite => UPDATE_MODE_OVERWRITE.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub enum Compression {
    Uncompressed,
    Lz4,
    Snappy,
    Zstd,
}

impl Compression {
    pub fn parse_from(name: &str) -> Result<Self> {
        if name.eq_ignore_ascii_case(COMPRESSION_UNCOMPRESSED) {
            Ok(Compression::Uncompressed)
        } else if name.eq_ignore_ascii_case(COMPRESSION_LZ4) {
            Ok(Compression::Lz4)
        } else if name.eq_ignore_ascii_case(COMPRESSION_SNAPPY) {
            Ok(Compression::Snappy)
        } else if name.eq_ignore_ascii_case(COMPRESSION_ZSTD) {
            Ok(Compression::Zstd)
        } else {
            ParseCompressionName { name }.fail()
        }
    }
}

impl ToString for Compression {
    fn to_string(&self) -> String {
        match self {
            Compression::Uncompressed => COMPRESSION_UNCOMPRESSED.to_string(),
            Compression::Lz4 => COMPRESSION_LZ4.to_string(),
            Compression::Snappy => COMPRESSION_SNAPPY.to_string(),
            Compression::Zstd => COMPRESSION_ZSTD.to_string(),
        }
    }
}

impl From<Compression> for manifest_pb::Compression {
    fn from(compression: Compression) -> Self {
        match compression {
            Compression::Uncompressed => manifest_pb::Compression::Uncompressed,
            Compression::Lz4 => manifest_pb::Compression::Lz4,
            Compression::Snappy => manifest_pb::Compression::Snappy,
            Compression::Zstd => manifest_pb::Compression::Zstd,
        }
    }
}

impl From<manifest_pb::Compression> for Compression {
    fn from(compression: manifest_pb::Compression) -> Self {
        match compression {
            manifest_pb::Compression::Uncompressed => Compression::Uncompressed,
            manifest_pb::Compression::Lz4 => Compression::Lz4,
            manifest_pb::Compression::Snappy => Compression::Snappy,
            manifest_pb::Compression::Zstd => Compression::Zstd,
        }
    }
}

impl From<Compression> for ParquetCompression {
    fn from(compression: Compression) -> Self {
        match compression {
            Compression::Uncompressed => ParquetCompression::UNCOMPRESSED,
            Compression::Lz4 => ParquetCompression::LZ4,
            Compression::Snappy => ParquetCompression::SNAPPY,
            Compression::Zstd => ParquetCompression::ZSTD(Default::default()),
        }
    }
}

/// A hint for building sst.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub enum StorageFormatHint {
    /// Which storage format is chosen to encode one sst depends on the data pattern
    #[default]
    Auto,
    Specific(StorageFormat),
}

/// StorageFormat specify how records are saved in persistent storage
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Serialize)]
pub enum StorageFormat {
    /// Traditional columnar format, every column is saved in one exact one column, for example:
    ///
    ///```plaintext
    /// | Timestamp | Device ID | Status Code | Tag 1 | Tag 2 |
    /// | --------- |---------- | ----------- | ----- | ----- |
    /// | 12:01     | A         | 0           | v1    | v1    |
    /// | 12:01     | B         | 0           | v2    | v2    |
    /// | 12:02     | A         | 0           | v1    | v1    |
    /// | 12:02     | B         | 1           | v2    | v2    |
    /// | 12:03     | A         | 0           | v1    | v1    |
    /// | 12:03     | B         | 0           | v2    | v2    |
    /// | .....     |           |             |       |       |
    /// ```
    Columnar,

    /// Design for time-series data
    /// Collapsible Columns within same primary key are collapsed into list, other columns are the same format with columnar's.
    ///
    /// Whether a column is collapsible is decided by `Schema::is_collapsible_column`
    ///
    /// Note: minTime/maxTime is optional and not implemented yet, mainly used for time-range pushdown filter
    ///
    ///```plaintext
    /// | Device ID | Timestamp           | Status Code | Tag 1 | Tag 2 | minTime | maxTime |
    /// |-----------|---------------------|-------------|-------|-------|---------|---------|
    /// | A         | [12:01,12:02,12:03] | [0,0,0]     | v1    | v1    | 12:01   | 12:03   |
    /// | B         | [12:01,12:02,12:03] | [0,1,0]     | v2    | v2    | 12:01   | 12:03   |
    /// | ...       |                     |             |       |       |         |         |
    /// ```
    Hybrid,
}

impl From<StorageFormatHint> for manifest_pb::StorageFormatHint {
    fn from(hint: StorageFormatHint) -> Self {
        match hint {
            StorageFormatHint::Auto => Self {
                hint: Some(manifest_pb::storage_format_hint::Hint::Auto(0)),
            },
            StorageFormatHint::Specific(format) => {
                let format = manifest_pb::StorageFormat::from(format);
                Self {
                    hint: Some(manifest_pb::storage_format_hint::Hint::Specific(
                        format as i32,
                    )),
                }
            }
        }
    }
}

impl TryFrom<manifest_pb::StorageFormatHint> for StorageFormatHint {
    type Error = Error;

    fn try_from(hint: manifest_pb::StorageFormatHint) -> Result<Self> {
        let format_hint = match hint.hint.context(MissingStorageFormatHint)? {
            manifest_pb::storage_format_hint::Hint::Auto(_) => StorageFormatHint::Auto,
            manifest_pb::storage_format_hint::Hint::Specific(format) => {
                let storage_format = manifest_pb::StorageFormat::from_i32(format)
                    .context(UnknownStorageFormatType { value: format })?;
                StorageFormatHint::Specific(storage_format.into())
            }
        };

        Ok(format_hint)
    }
}

impl ToString for StorageFormatHint {
    fn to_string(&self) -> String {
        match self {
            Self::Auto => STORAGE_FORMAT_AUTO.to_string(),
            Self::Specific(format) => format.to_string(),
        }
    }
}

impl TryFrom<&str> for StorageFormatHint {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let format = match value.to_uppercase().as_str() {
            STORAGE_FORMAT_COLUMNAR => Self::Specific(StorageFormat::Columnar),
            STORAGE_FORMAT_HYBRID => Self::Specific(StorageFormat::Hybrid),
            STORAGE_FORMAT_AUTO => Self::Auto,
            _ => return UnknownStorageFormatHint { value }.fail(),
        };
        Ok(format)
    }
}

impl From<StorageFormat> for manifest_pb::StorageFormat {
    fn from(format: StorageFormat) -> Self {
        match format {
            StorageFormat::Columnar => Self::Columnar,
            StorageFormat::Hybrid => Self::Hybrid,
        }
    }
}

impl From<manifest_pb::StorageFormat> for StorageFormat {
    fn from(format: manifest_pb::StorageFormat) -> Self {
        match format {
            manifest_pb::StorageFormat::Columnar => Self::Columnar,
            manifest_pb::StorageFormat::Hybrid => Self::Hybrid,
        }
    }
}

impl TryFrom<&str> for StorageFormat {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let format = match value.to_uppercase().as_str() {
            STORAGE_FORMAT_COLUMNAR => Self::Columnar,
            STORAGE_FORMAT_HYBRID => Self::Hybrid,
            _ => return UnknownStorageFormat { value }.fail(),
        };
        Ok(format)
    }
}

impl ToString for StorageFormat {
    fn to_string(&self) -> String {
        match self {
            Self::Columnar => STORAGE_FORMAT_COLUMNAR,
            Self::Hybrid => STORAGE_FORMAT_HYBRID,
        }
            .to_string()
    }
}

impl Default for StorageFormat {
    fn default() -> Self {
        Self::Columnar
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Serialize)]
#[serde(default)]
pub struct TableOptions {
    // The following options are immutable once table was created.

    /// Segment duration of the table. 默认 DEFAULT_SEGMENT_DURATION 7200s
    ///
    /// `None` means the table is doing the segment duration sampling and the actual duration is still unknown.
    pub segmentDuration: Option<ReadableDuration>,

    /// updateMode默认是override需要去重的 fenquen
    pub update_mode: UpdateMode,

    /// Hint for storage format.
    pub storage_format_hint: StorageFormatHint,

    // The following options can be altered.

    /// Enable ttl
    pub enable_ttl: bool,
    /// Time-to-live of the data.
    pub ttl: ReadableDuration,
    /// Arena block size of memtable.
    pub arena_block_size: u32,
    /// Write buffer size of memtable.
    pub write_buffer_size: u32,
    /// Compaction strategy of the table.
    pub compaction_strategy: CompactionStrategy,
    /// Row number in a row group.
    pub num_rows_per_row_group: usize,
    /// Table Compression
    pub compression: Compression,
}

impl TableOptions {
    #[inline]
    pub fn segment_duration(&self) -> Option<Duration> {
        self.segmentDuration.map(|v| v.0)
    }

    #[inline]
    pub fn ttl(&self) -> Option<ReadableDuration> {
        if self.enable_ttl {
            Some(self.ttl)
        } else {
            None
        }
    }

    // for show create table
    pub fn to_raw_map(&self) -> HashMap<String, String> {
        let mut m = [
            (SEGMENT_DURATION.to_string(), self.segmentDuration.map(|v| v.to_string()).unwrap_or_else(String::new), ),
            (UPDATE_MODE.to_string(), self.update_mode.to_string()),
            (ENABLE_TTL.to_string(), self.enable_ttl.to_string()),
            (TTL.to_string(), format!("{}", self.ttl)),
            (ARENA_BLOCK_SIZE.to_string(), format!("{}", self.arena_block_size)),
            (WRITE_BUFFER_SIZE.to_string(), format!("{}", self.write_buffer_size)),
            (NUM_ROWS_PER_ROW_GROUP.to_string(), format!("{}", self.num_rows_per_row_group)),
            (COMPRESSION.to_string(), self.compression.to_string()),
            (STORAGE_FORMAT.to_string(), self.storage_format_hint.to_string(), ),
        ].into_iter().collect();
        self.compaction_strategy.fill_raw_map(&mut m);

        m
    }

    /// Sanitize options silently.
    pub fn sanitize(&mut self) {
        let one_day_secs = BUCKET_DURATION_1D.as_secs();

        if let Some(segment_duration) = self.segmentDuration {
            let mut segment_duration_secs = segment_duration.as_secs();
            if segment_duration_secs == 0 {
                segment_duration_secs = DEFAULT_SEGMENT_DURATION.as_secs()
            };
            self.segmentDuration = Some(ReadableDuration::secs(segment_duration_secs));
        }

        let ttl_secs = self.ttl.as_secs();
        // Ttl must align to day.
        let ttl_secs = ttl_secs / one_day_secs * one_day_secs;
        self.ttl = ReadableDuration::secs(ttl_secs);

        if self.arena_block_size < MIN_ARENA_BLOCK_SIZE {
            self.arena_block_size = MIN_ARENA_BLOCK_SIZE;
        }

        if self.arena_block_size > MAX_ARENA_BLOCK_SIZE {
            self.arena_block_size = MAX_ARENA_BLOCK_SIZE;
        }

        if self.num_rows_per_row_group < MIN_NUM_ROWS_PER_ROW_GROUP {
            self.num_rows_per_row_group = MIN_NUM_ROWS_PER_ROW_GROUP;
        }

        if self.num_rows_per_row_group > MAX_NUM_ROWS_PER_ROW_GROUP {
            self.num_rows_per_row_group = MAX_NUM_ROWS_PER_ROW_GROUP;
        }
    }

    pub fn needDeDuplicate(&self) -> bool {
        match self.update_mode {
            UpdateMode::Overwrite => true,
            UpdateMode::Append => false,
        }
    }

    pub fn is_expired(&self, timestamp: Timestamp) -> bool {
        self.enable_ttl && timestamp.is_expired(Timestamp::expire_time(self.ttl.0))
    }
}

impl From<SizeTieredCompactionOptions> for manifest_pb::CompactionOptions {
    fn from(opts: SizeTieredCompactionOptions) -> Self {
        manifest_pb::CompactionOptions {
            bucket_low: opts.bucket_low,
            bucket_high: opts.bucket_high,
            min_sstable_size: opts.min_sstable_size.0 as u32,
            min_threshold: opts.min_threshold as u32,
            max_threshold: opts.max_threshold as u32,
            // FIXME: Is it ok to use the default timestamp resolution here?
            timestamp_resolution: manifest_pb::TimeUnit::Nanoseconds as i32,
        }
    }
}

impl From<manifest_pb::CompactionOptions> for SizeTieredCompactionOptions {
    fn from(opts: manifest_pb::CompactionOptions) -> Self {
        Self {
            bucket_low: opts.bucket_low,
            bucket_high: opts.bucket_high,
            min_sstable_size: ReadableSize(opts.min_sstable_size as u64),
            min_threshold: opts.min_threshold as usize,
            max_threshold: opts.max_threshold as usize,
            max_input_sstable_size: compaction::get_max_input_sstable_size(),
        }
    }
}

impl From<TimeWindowCompactionOptions> for manifest_pb::CompactionOptions {
    fn from(v: TimeWindowCompactionOptions) -> Self {
        manifest_pb::CompactionOptions {
            bucket_low: v.size_tiered.bucket_low,
            bucket_high: v.size_tiered.bucket_high,
            min_sstable_size: v.size_tiered.min_sstable_size.0 as u32,
            min_threshold: v.size_tiered.min_threshold as u32,
            max_threshold: v.size_tiered.max_threshold as u32,
            timestamp_resolution: manifest_pb::TimeUnit::from(v.timestamp_resolution) as i32,
        }
    }
}

impl From<manifest_pb::CompactionOptions> for TimeWindowCompactionOptions {
    fn from(opts: manifest_pb::CompactionOptions) -> Self {
        let size_tiered: SizeTieredCompactionOptions = opts.clone().into();

        Self {
            size_tiered,
            timestamp_resolution: TimeUnit::from(opts.timestamp_resolution()),
        }
    }
}

impl From<TableOptions> for manifest_pb::TableOptions {
    fn from(opts: TableOptions) -> Self {
        let segment_duration = opts
            .segmentDuration
            .map(|v| v.0.as_millis_u64())
            .unwrap_or(0);
        let sampling_segment_duration = opts.segmentDuration.is_none();

        let (compaction_strategy, compaction_options) = match opts.compaction_strategy {
            CompactionStrategy::Default => (manifest_pb::CompactionStrategy::Default, None),
            CompactionStrategy::SizeTiered(v) => (
                manifest_pb::CompactionStrategy::SizeTiered,
                Some(manifest_pb::CompactionOptions::from(v)),
            ),
            CompactionStrategy::TimeWindow(v) => (
                manifest_pb::CompactionStrategy::TimeWindow,
                Some(manifest_pb::CompactionOptions::from(v)),
            ),
        };

        manifest_pb::TableOptions {
            segment_duration,
            enable_ttl: opts.enable_ttl,
            ttl: opts.ttl.0.as_millis_u64(),
            arena_block_size: opts.arena_block_size,
            num_rows_per_row_group: opts.num_rows_per_row_group as u64,
            compaction_strategy: compaction_strategy as i32,
            compaction_options,
            update_mode: manifest_pb::UpdateMode::from(opts.update_mode) as i32,
            write_buffer_size: opts.write_buffer_size,
            compression: manifest_pb::Compression::from(opts.compression) as i32,
            sampling_segment_duration,
            storage_format_hint: Some(manifest_pb::StorageFormatHint::from(
                opts.storage_format_hint,
            )),
        }
    }
}

impl From<UpdateMode> for manifest_pb::UpdateMode {
    fn from(v: UpdateMode) -> Self {
        match v {
            UpdateMode::Overwrite => manifest_pb::UpdateMode::Overwrite,
            UpdateMode::Append => manifest_pb::UpdateMode::Append,
        }
    }
}

impl From<manifest_pb::UpdateMode> for UpdateMode {
    fn from(v: manifest_pb::UpdateMode) -> Self {
        match v {
            manifest_pb::UpdateMode::Overwrite => UpdateMode::Overwrite,
            manifest_pb::UpdateMode::Append => UpdateMode::Append,
        }
    }
}

impl TryFrom<manifest_pb::TableOptions> for TableOptions {
    type Error = Error;

    fn try_from(opts: manifest_pb::TableOptions) -> Result<Self> {
        let compression = opts.compression();
        let update_mode = opts.update_mode();

        let compaction_strategy = match opts.compaction_strategy() {
            manifest_pb::CompactionStrategy::Default => CompactionStrategy::default(),
            manifest_pb::CompactionStrategy::SizeTiered => {
                let opts = opts
                    .compaction_options
                    .map(SizeTieredCompactionOptions::from)
                    .unwrap_or_default();
                CompactionStrategy::SizeTiered(opts)
            }
            manifest_pb::CompactionStrategy::TimeWindow => {
                let opts = opts
                    .compaction_options
                    .map(TimeWindowCompactionOptions::from)
                    .unwrap_or_default();
                CompactionStrategy::TimeWindow(opts)
            }
        };

        let segment_duration = if opts.sampling_segment_duration {
            None
        } else if opts.segment_duration == 0 {
            // If segment duration is still zero. If the data had been used by an elder
            // version release that not yet support sampling, the
            // `sampling_segment_duration` flag would be truncated after
            // manifest snapshot, but left segment duration zero.
            Some(DEFAULT_SEGMENT_DURATION.into())
        } else {
            Some(Duration::from_millis(opts.segment_duration).into())
        };

        let storage_format_hint = opts.storage_format_hint.context(MissingStorageFormatHint)?;
        let table_opts = Self {
            segmentDuration: segment_duration,
            enable_ttl: opts.enable_ttl,
            ttl: Duration::from_millis(opts.ttl).into(),
            arena_block_size: opts.arena_block_size,
            compaction_strategy,
            num_rows_per_row_group: opts.num_rows_per_row_group as usize,
            update_mode: UpdateMode::from(update_mode),
            write_buffer_size: opts.write_buffer_size,
            compression: Compression::from(compression),
            storage_format_hint: StorageFormatHint::try_from(storage_format_hint)?,
        };

        Ok(table_opts)
    }
}

impl Default for TableOptions {
    fn default() -> Self {
        Self {
            segmentDuration: None,
            enable_ttl: true,
            ttl: DEFAULT_TTL.into(),
            arena_block_size: DEFAULT_ARENA_BLOCK_SIZE,
            compaction_strategy: CompactionStrategy::default(),
            num_rows_per_row_group: DEFAULT_NUM_ROW_PER_ROW_GROUP,
            update_mode: UpdateMode::Overwrite,
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            compression: Compression::Zstd,
            storage_format_hint: StorageFormatHint::default(),
        }
    }
}

pub fn merge_table_options_for_create(options: &HashMap<String, String>,
                                      table_opts: &TableOptions) -> Result<TableOptions> {
    merge_table_options(options, table_opts, true)
}

pub fn merge_table_options_for_alter(options: &HashMap<String, String>,
                                     table_opts: &TableOptions) -> Result<TableOptions> {
    merge_table_options(options, table_opts, false)
}

/// The options will override the old options.
fn merge_table_options(options: &HashMap<String, String>,
                       table_old_opts: &TableOptions,
                       is_create: bool) -> Result<TableOptions> {
    let mut table_opts = table_old_opts.clone();
    if is_create {
        if let Some(v) = options.get(SEGMENT_DURATION) {
            table_opts.segmentDuration = Some(parse_duration(v).context(ParseDuration)?);
        }
        if let Some(v) = options.get(UPDATE_MODE) {
            table_opts.update_mode = UpdateMode::parse_from(v)?;
        }
    }

    if let Some(v) = options.get(TTL) {
        table_opts.ttl = parse_duration(v).context(ParseDuration)?;
    }
    if let Some(v) = options.get(OPTION_KEY_ENABLE_TTL) {
        table_opts.enable_ttl = v.parse::<bool>().context(ParseBool)?;
    }
    if let Some(v) = options.get(ARENA_BLOCK_SIZE) {
        let size = parse_size(v)?;
        table_opts.arena_block_size = size.0 as u32;
    }
    if let Some(v) = options.get(WRITE_BUFFER_SIZE) {
        let size = parse_size(v)?;
        table_opts.write_buffer_size = size.0 as u32;
    }
    if let Some(v) = options.get(COMPACTION_STRATEGY) {
        table_opts.compaction_strategy =
            CompactionStrategy::parse_from(v, options).context(ParseStrategy { value: v })?;
    }
    if let Some(v) = options.get(NUM_ROWS_PER_ROW_GROUP) {
        table_opts.num_rows_per_row_group = v.parse().context(ParseInt)?;
    }
    if let Some(v) = options.get(COMPRESSION) {
        table_opts.compression = Compression::parse_from(v)?;
    }
    if let Some(v) = options.get(STORAGE_FORMAT) {
        table_opts.storage_format_hint = v.as_str().try_into()?;
    }
    Ok(table_opts)
}

fn parse_size(v: &str) -> Result<ReadableSize> {
    v.parse::<ReadableSize>().map_err(|err| Error::ParseSize {
        err,
        backtrace: Backtrace::generate(),
    })
}
