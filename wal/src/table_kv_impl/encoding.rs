// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Constants and utils for encoding.

use chrono::{TimeZone, Utc};
use common_types::{table::TableId, time::Timestamp};
use macros::define_result;
use snafu::Snafu;
use table_kv::{KeyBoundary, ScanRequest};
use time_ext::ReadableDuration;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Timestamp is invalid, timestamp:{}", timestamp))]
    InValidTimestamp { timestamp: i64 },
}

define_result!(Error);

/// Key prefix for namespace in meta table.
const META_NAMESPACE_PREFIX: &str = "v1/namespace";
/// Key prefix for bucket in meta table.
const META_BUCKET_PREFIX: &str = "v1/bucket";
/// Format of bucket timestamp.
const BUCKET_TIMESTAMP_FORMAT: &str = "%Y-%m-%dT%H:%M:%S";
/// Key prefix of table unit meta.
const TABLE_UNIT_META_PREFIX: &str = "v1/table";
/// Format of timestamp in wal table name.
const WAL_SHARD_TIMESTAMP_FORMAT: &str = "%Y%m%d%H%M%S";

#[inline]
pub fn scan_request_for_prefix(prefix: &str) -> ScanRequest {
    ScanRequest {
        start: KeyBoundary::excluded(prefix.as_bytes()),
        end: KeyBoundary::max_included(),
        reverse: false,
    }
}

#[inline]
pub fn format_namespace_key(namespace: &str) -> String {
    format!("{META_NAMESPACE_PREFIX}/{namespace}")
}

#[inline]
pub fn bucket_key_prefix(namespace: &str) -> String {
    format!("{META_BUCKET_PREFIX}/{namespace}/")
}

pub fn format_timed_bucket_key(
    namespace: &str,
    bucket_duration: ReadableDuration,
    gmt_start_ms: Timestamp,
) -> Result<String> {
    let duration = bucket_duration.to_string();
    let dt = match Utc.timestamp_millis_opt(gmt_start_ms.as_i64()).single() {
        None => InValidTimestamp {
            timestamp: gmt_start_ms.as_i64(),
        }
        .fail()?,
        Some(v) => v,
    };
    Ok(format!(
        "{}/{}/{}/{}",
        META_BUCKET_PREFIX,
        namespace,
        duration,
        dt.format(BUCKET_TIMESTAMP_FORMAT)
    ))
}

pub fn format_permanent_bucket_key(namespace: &str) -> String {
    format!("{META_BUCKET_PREFIX}/{namespace}/permanent")
}

#[inline]
pub fn format_table_unit_meta_name(namespace: &str, shard_id: usize) -> String {
    format!("table_unit_meta_{namespace}_{shard_id:0>6}")
}

#[inline]
pub fn format_timed_wal_name(
    namespace: &str,
    gmt_start_ms: Timestamp,
    shard_id: usize,
) -> Result<String> {
    let dt = match Utc.timestamp_millis_opt(gmt_start_ms.as_i64()).single() {
        None => InValidTimestamp {
            timestamp: gmt_start_ms.as_i64(),
        }
        .fail()?,
        Some(v) => v,
    };
    Ok(format!(
        "wal_{}_{}_{:0>6}",
        namespace,
        dt.format(WAL_SHARD_TIMESTAMP_FORMAT),
        shard_id
    ))
}

#[inline]
pub fn format_permanent_wal_name(namespace: &str, shard_id: usize) -> String {
    format!("wal_{namespace}_permanent_{shard_id:0>6}")
}

#[inline]
pub fn format_table_unit_key(table_id: TableId) -> String {
    format!("{TABLE_UNIT_META_PREFIX}/{table_id}")
}