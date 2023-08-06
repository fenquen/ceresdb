// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Data models for TableKv based wal.

use std::{str, time::Duration};

use common_types::{table::TableId, time::Timestamp};
use macros::define_result;
use serde::{Deserialize, Serialize};
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use table_kv::ScanContext;
use time_ext::ReadableDuration;

use crate::{
    manager::SequenceNumber,
    table_kv_impl::{consts, table_unit::CleanContext},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Invalid name, name:{}, reason:{}.\nBacktrace:\n{}",
        name,
        reason,
        backtrace
    ))]
    InvalidName {
        name: String,
        reason: &'static str,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid ttl, namespace:{}, ttl:{:?}.\nBacktrace:\n{}",
        namespace,
        ttl,
        backtrace
    ))]
    InvalidTtl {
        namespace: String,
        ttl: ReadableDuration,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Too much wal shards, namespace:{}, shard_num:{}.\nBacktrace:\n{}",
        namespace,
        shard_num,
        backtrace
    ))]
    TooMuchWalShards {
        namespace: String,
        shard_num: usize,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Too much table unit meta shards, namespace:{}, shard_num:{}.\nBacktrace:\n{}",
        namespace,
        shard_num,
        backtrace
    ))]
    TooMuchTableUnitMetaShards {
        namespace: String,
        shard_num: usize,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid utf8 string, err:{}.\nBacktrace:\n{}", source, backtrace))]
    InvalidUtf8 {
        source: std::str::Utf8Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid json, err:{}, json:{}.\nBacktrace:\n{}",
        source,
        json,
        backtrace
    ))]
    InvalidJson {
        json: String,
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to encode json, err:{}.\nBacktrace:\n{}", source, backtrace))]
    EncodeJson {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
}

define_result!(Error);

const DEFAULT_WAL_SHARD_NUM: usize = 512;
const DEFAULT_TTL_DAYS: u64 = 1;
const DEFAULT_TABLE_UNIT_META_SHARD_NUM: usize = 128;
const MAX_NAME_LEN: usize = 16;

fn validate_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty(),
        InvalidName {
            name,
            reason: "Name is empty",
        }
    );

    ensure!(
        name.len() <= MAX_NAME_LEN,
        InvalidName {
            name,
            reason: "Name too long",
        }
    );

    let mut contains_alphanumeric = false;
    for ch in name.chars() {
        ensure!(
            ch.is_ascii_alphanumeric() || ch == '_',
            InvalidName {
                name,
                reason: "Name should only contains alphanumeric or '_' character",
            }
        );
        if ch.is_ascii_alphanumeric() {
            contains_alphanumeric = true;
        }
    }

    ensure!(
        contains_alphanumeric,
        InvalidName {
            name,
            reason: "Name must contains alphanumeric character",
        }
    );

    Ok(())
}

fn decode_json<'a, T: serde::Deserialize<'a>>(data: &'a [u8]) -> Result<T> {
    let json = str::from_utf8(data).context(InvalidUtf8)?;
    serde_json::from_str(json).context(InvalidJson { json })
}

fn encode_json<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    let json = serde_json::to_string(value).context(EncodeJson)?;
    Ok(json.into_bytes())
}

/// Data of wal shards.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct WalShardEntry {
    /// Whether ttl of wal shard is enabled.
    pub enable_ttl: bool,
    /// Ttl of wal shards, only takes effect if `enable_ttl` is true.
    pub ttl: Option<ReadableDuration>,
    /// Hash shard num of wal.
    pub shard_num: usize,
}

impl Default for WalShardEntry {
    fn default() -> Self {
        Self {
            enable_ttl: true,
            ttl: Some(ReadableDuration::days(DEFAULT_TTL_DAYS)),
            shard_num: DEFAULT_WAL_SHARD_NUM,
        }
    }
}

/// Data of table unit meta tables.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TableUnitMetaEntry {
    /// Hash shard num of table unit meta tables.
    pub shard_num: usize,
}

impl Default for TableUnitMetaEntry {
    fn default() -> Self {
        Self {
            shard_num: DEFAULT_TABLE_UNIT_META_SHARD_NUM,
        }
    }
}

/// Data of a wal namespace, which is similar to a wal directory.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct NamespaceEntry {
    /// Name of namespace.
    pub name: String,
    /// Wal shard datas.
    pub wal: WalShardEntry,
    /// Table unit meta shard datas.
    pub table_unit_meta: TableUnitMetaEntry,
}

impl NamespaceEntry {
    fn validate(&self) -> Result<()> {
        validate_name(&self.name)?;

        // Validate ttl.
        if let Some(wal_ttl) = self.wal.ttl {
            ensure!(
                !wal_ttl.is_zero(),
                InvalidTtl {
                    namespace: &self.name,
                    ttl: wal_ttl,
                }
            );

            let ttl_ms = wal_ttl.as_millis();
            ensure!(
                ttl_ms % consts::DAY_MS == 0,
                InvalidTtl {
                    namespace: &self.name,
                    ttl: wal_ttl,
                }
            );
        }

        Ok(())
    }

    #[inline]
    pub fn decode(data: &[u8]) -> Result<Self> {
        decode_json(data)
    }

    #[inline]
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_json(self)
    }
}

impl Default for NamespaceEntry {
    fn default() -> Self {
        Self {
            name: "wal".to_string(),
            wal: WalShardEntry::default(),
            table_unit_meta: TableUnitMetaEntry::default(),
        }
    }
}

/// Namespace config.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NamespaceConfig {
    pub wal_shard_num: usize,
    pub table_unit_meta_shard_num: usize,

    /// Outdated log cleaning strategy
    ///  
    /// NOTICE: you can just set once, if change after setting, it will lead to
    /// error. Recommend to use default setting.
    pub ttl: Option<ReadableDuration>,

    pub init_scan_timeout: ReadableDuration,
    pub init_scan_batch_size: i32,
    pub clean_scan_timeout: ReadableDuration,
    pub clean_scan_batch_size: usize,
}

impl NamespaceConfig {
    pub fn sanitize(&mut self) {
        if self.init_scan_batch_size <= 0 {
            self.init_scan_batch_size = ScanContext::DEFAULT_BATCH_SIZE;
        }
    }

    pub fn new_namespace_entry(&self, namespace_name: &str) -> Result<NamespaceEntry> {
        let entry = NamespaceEntry {
            name: namespace_name.to_string(),
            wal: WalShardEntry {
                enable_ttl: self.ttl.is_some(),
                ttl: self.ttl,
                shard_num: self.wal_shard_num,
            },
            table_unit_meta: TableUnitMetaEntry {
                shard_num: self.table_unit_meta_shard_num,
            },
        };

        entry.validate()?;

        Ok(entry)
    }

    pub fn new_init_scan_ctx(&self) -> ScanContext {
        ScanContext {
            timeout: self.init_scan_timeout.0,
            batch_size: self.init_scan_batch_size,
        }
    }

    #[inline]
    pub fn new_bucket_scan_ctx(&self) -> ScanContext {
        self.new_init_scan_ctx()
    }

    pub fn new_clean_ctx(&self) -> CleanContext {
        CleanContext {
            scan_timeout: self.clean_scan_timeout.0,
            batch_size: self.clean_scan_batch_size,
        }
    }
}

impl Default for NamespaceConfig {
    fn default() -> Self {
        let default_clean_ctx = CleanContext::default();

        Self {
            wal_shard_num: DEFAULT_WAL_SHARD_NUM,
            table_unit_meta_shard_num: DEFAULT_TABLE_UNIT_META_SHARD_NUM,
            ttl: Some(ReadableDuration::days(DEFAULT_TTL_DAYS)),

            init_scan_timeout: ReadableDuration::secs(10),
            init_scan_batch_size: 100,
            clean_scan_timeout: default_clean_ctx.scan_timeout.into(),
            clean_scan_batch_size: default_clean_ctx.batch_size,
        }
    }
}

/// Contains all wal shards of given time range, table unit is routed to a
/// specific shard by its region id.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct BucketEntry {
    /// Shard number of the bucket.
    pub shard_num: usize,
    /// Start time of the bucket.
    #[serde(with = "format_timestamp")]
    gmt_start_ms: Timestamp,
    /// Exclusive end time of the bucket, now we use Timestamp::MAX to denote
    /// this is a permanent bucket.
    ///
    /// We make this field private to avoid user modifying it.
    #[serde(with = "format_timestamp")]
    gmt_end_ms: Timestamp,
}

impl Default for BucketEntry {
    fn default() -> Self {
        Self {
            shard_num: DEFAULT_WAL_SHARD_NUM,
            gmt_start_ms: Timestamp::ZERO,
            gmt_end_ms: Timestamp::ZERO,
        }
    }
}

impl BucketEntry {
    /// Create a timed bucket.
    pub fn new_timed(
        shard_num: usize,
        gmt_start_ms: Timestamp,
        bucket_duration_ms: i64,
    ) -> Option<Self> {
        let gmt_end_ms = gmt_start_ms.checked_add_i64(bucket_duration_ms)?;

        Some(Self {
            shard_num,
            gmt_start_ms,
            gmt_end_ms,
        })
    }

    /// Create a permanent bucket.
    pub fn new_permanent(shard_num: usize) -> Self {
        Self {
            shard_num,
            gmt_start_ms: Timestamp::ZERO,
            gmt_end_ms: Timestamp::MAX,
        }
    }

    #[inline]
    pub fn decode(data: &[u8]) -> Result<Self> {
        decode_json(data)
    }

    #[inline]
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_json(self)
    }

    #[inline]
    pub fn gmt_start_ms(&self) -> Timestamp {
        self.gmt_start_ms
    }

    #[inline]
    pub fn gmt_end_ms(&self) -> Timestamp {
        self.gmt_end_ms
    }

    /// Returns true if this is a permanent bucket.
    #[inline]
    pub fn is_permanent(&self) -> bool {
        self.gmt_end_ms == Timestamp::MAX
    }

    /// Returns the duration of the bucket, or None if this is a permanent
    /// bucket that do not have a ttl.
    ///
    /// If `gmt_end_ms == gmt_start_ms`, returns zero duration.
    pub fn bucket_duration(&self) -> Option<Duration> {
        if self.is_permanent() {
            None
        } else {
            assert!(self.gmt_end_ms >= self.gmt_start_ms);
            // The above assertion ensure duration >= 0.
            let duration_ms = (self.gmt_end_ms.as_i64() - self.gmt_start_ms.as_i64()) as u64;

            Some(Duration::from_millis(duration_ms))
        }
    }

    /// Returns true if this is a timed bucket and it is expired.
    #[inline]
    pub fn is_expired(&self, min_timestamp_not_expired: Timestamp) -> bool {
        if self.is_permanent() {
            return false;
        }

        // End is exclusive.
        self.gmt_end_ms <= min_timestamp_not_expired
    }
}

/// Meta data of a table unit.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TableUnitEntry {
    #[serde(with = "format_string")]
    pub table_id: TableId,
    #[serde(with = "format_string")]
    pub start_sequence: SequenceNumber,
    // TODO(yingwen): We can store last wal shard name when writing to this
    // entry, so we can skip earlier shards during searching last sequence.
}

impl Default for TableUnitEntry {
    fn default() -> Self {
        Self {
            table_id: 0,
            start_sequence: common_types::MIN_SEQUENCE_NUMBER,
        }
    }
}

impl TableUnitEntry {
    pub fn new(table_id: TableId) -> TableUnitEntry {
        TableUnitEntry {
            table_id,
            ..Default::default()
        }
    }

    #[inline]
    pub fn decode(data: &[u8]) -> Result<Self> {
        decode_json(data)
    }

    #[inline]
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_json(self)
    }
}

mod format_timestamp {
    use common_types::time::Timestamp;
    use serde::{de, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Timestamp, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(&value.as_i64())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Timestamp, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)?;
        Ok(Timestamp::new(value))
    }
}

mod format_string {
    use std::{fmt::Display, str::FromStr};

    use serde::{de, Deserialize, Deserializer, Serializer};

    pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: Display,
        S: Serializer,
    {
        serializer.collect_str(value)
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        T: FromStr,
        T::Err: Display,
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}