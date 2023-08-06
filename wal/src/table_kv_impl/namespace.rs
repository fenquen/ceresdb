// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Wal namespace.

use std::{
    collections::{BTreeMap, HashMap},
    fmt, str,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use common_types::{table::TableId, time::Timestamp};
use generic_error::{BoxError, GenericError};
use log::{debug, error, info, trace, warn};
use macros::define_result;
use runtime::Runtime;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_kv::{
    ScanContext as KvScanContext, ScanIter, TableError, TableKv, WriteBatch, WriteContext,
};
use time_ext::ReadableDuration;
use timed_task::{TaskHandle, TimedTask};

use crate::{
    kv_encoder::CommonLogKey,
    log_batch::LogWriteBatch,
    manager::{
        self, ReadContext, ReadRequest, RegionId, ScanContext, ScanRequest, SequenceNumber,
        WalLocation,
    },
    table_kv_impl::{
        consts, encoding,
        model::{BucketEntry, NamespaceConfig, NamespaceEntry},
        table_unit::{TableLogIterator, TableUnit, TableUnitRef},
        WalRuntimes,
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create table, err:{}", source,))]
    CreateTable { source: GenericError },

    #[snafu(display(
        "Failed to init table unit meta, namespace:{}, err:{}",
        namespace,
        source,
    ))]
    InitTableUnitMeta {
        namespace: String,
        source: GenericError,
    },

    #[snafu(display("Failed to load buckets, namespace:{}, err:{}", namespace, source,))]
    LoadBuckets {
        namespace: String,
        source: GenericError,
    },

    #[snafu(display("Failed to open bucket, namespace:{}, err:{}", namespace, source,))]
    BucketMeta {
        namespace: String,
        source: GenericError,
    },

    #[snafu(display(
        "Bucket timestamp out of range, namespace:{}, timestamp:{:?}.\nBacktrace:\n{}",
        namespace,
        timestamp,
        backtrace
    ))]
    BucketOutOfRange {
        namespace: String,
        timestamp: Timestamp,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to drop bucket shard, namespace:{}, err:{}", namespace, source,))]
    DropShard {
        namespace: String,
        source: GenericError,
    },

    #[snafu(display("Failed to encode entry, namespace:{}, err:{}", namespace, source,))]
    Encode {
        namespace: String,
        source: crate::table_kv_impl::model::Error,
    },

    #[snafu(display("Failed to decode entry, key:{}, err:{}", key, source,))]
    Decode {
        key: String,
        source: crate::table_kv_impl::model::Error,
    },

    #[snafu(display("Failed to persist value, key:{}, err:{}", key, source,))]
    PersistValue { key: String, source: GenericError },

    #[snafu(display(
        "Failed to purge bucket, namespace:{}, msg:{}, err:{}",
        namespace,
        msg,
        source,
    ))]
    PurgeBucket {
        namespace: String,
        msg: String,
        source: GenericError,
    },

    #[snafu(display("Failed to get value, key:{}, err:{}", key, source,))]
    GetValue { key: String, source: GenericError },

    #[snafu(display("Value not found, key:{}.\nBacktrace:\n{}", key, backtrace))]
    ValueNotFound { key: String, backtrace: Backtrace },

    #[snafu(display("Failed to build namespace, namespace:{}, err:{}", namespace, source,))]
    BuildNamespace {
        namespace: String,
        source: crate::table_kv_impl::model::Error,
    },

    #[snafu(display(
        "Failed to open region, namespace:{}, region_id:{}, table_id:{}, err:{}",
        namespace,
        region_id,
        table_id,
        source
    ))]
    OpenTableUnit {
        namespace: String,
        region_id: u64,
        table_id: TableId,
        source: crate::table_kv_impl::table_unit::Error,
    },

    #[snafu(display(
        "Failed to create table unit, namespace:{}, wal location:{:?}, err:{}",
        namespace,
        location,
        source
    ))]
    CreateTableUnit {
        namespace: String,
        location: WalLocation,
        source: crate::table_kv_impl::table_unit::Error,
    },

    #[snafu(display(
        "Failed to write table unit, namespace:{}, wal location:{:?}, err:{}",
        namespace,
        location,
        source
    ))]
    WriteTableUnit {
        namespace: String,
        location: WalLocation,
        source: crate::table_kv_impl::table_unit::Error,
    },

    #[snafu(display(
        "Failed to read table unit, namespace:{}, wal location:{:?}, err:{}",
        namespace,
        location,
        source
    ))]
    ReadTableUnit {
        namespace: String,
        location: WalLocation,
        source: crate::table_kv_impl::table_unit::Error,
    },

    #[snafu(display(
        "Failed to delete entries, namespace:{}, wal location:{:?}, err:{}",
        namespace,
        location,
        source
    ))]
    DeleteEntries {
        namespace: String,
        location: WalLocation,
        source: crate::table_kv_impl::table_unit::Error,
    },

    #[snafu(display("Failed to stop task, namespace:{}, err:{}", namespace, source))]
    StopTask {
        namespace: String,
        source: runtime::Error,
    },

    #[snafu(display(
        "Failed to clean deleted logs, namespace:{}, region_id:{}, table_id:{}, err:{}",
        namespace,
        region_id,
        table_id,
        source
    ))]
    CleanLog {
        namespace: String,
        region_id: u64,
        table_id: TableId,
        source: crate::table_kv_impl::table_unit::Error,
    },
}

define_result!(Error);

/// Duration of a bucket (1d).
pub const BUCKET_DURATION_MS: i64 = 1000 * 3600 * 24;
/// Check whether to create a new bucket every `BUCKET_DURATION_PERIOD`.
const BUCKET_MONITOR_PERIOD: Duration = Duration::from_millis(BUCKET_DURATION_MS as u64 / 8);
/// Clean deleted logs period.
const LOG_CLEANER_PERIOD: Duration = Duration::from_millis(BUCKET_DURATION_MS as u64 / 4);

struct NamespaceInner<T> {
    runtimes: WalRuntimes,
    table_kv: T,
    entry: NamespaceEntry,
    bucket_set: RwLock<BucketSet>,
    // TODO: should use some strategies(such as lru) to clean the invalid table unit.
    table_units: RwLock<HashMap<WalLocation, TableUnitRef>>,
    meta_table_name: String,
    table_unit_meta_tables: Vec<String>,
    operator: Mutex<TableOperator>,
    // Only one thread can persist and create a new bucket.
    bucket_creator: Mutex<BucketCreator>,
    config: NamespaceConfig,
}

impl<T> NamespaceInner<T> {
    #[inline]
    pub fn name(&self) -> &str {
        &self.entry.name
    }

    /// Names of region meta tables.
    fn table_unit_meta_tables(&self) -> &[String] {
        &self.table_unit_meta_tables
    }

    fn table_unit_meta_table(&self, table_id: TableId) -> &str {
        let index = table_id as usize % self.table_unit_meta_tables.len();

        &self.table_unit_meta_tables[index]
    }

    fn list_buckets(&self) -> Vec<BucketRef> {
        self.bucket_set.read().unwrap().buckets()
    }

    fn list_table_units(&self) -> Vec<TableUnitRef> {
        self.table_units.read().unwrap().values().cloned().collect()
    }

    fn clear_table_units(&self) {
        let mut table_units = self.table_units.write().unwrap();
        table_units.clear();
    }

    fn get_statistics(&self) -> String {
        let table_units = self.table_units.read().unwrap();
        let mut wal_stats = Vec::with_capacity(table_units.len());
        for table_unit in table_units.values() {
            wal_stats.push(format!("{:?}", table_unit.as_ref()));
        }
        let stats = wal_stats.join("\n");

        let stats = format!("#TableKvWal stats:\n{stats}\n");

        stats
    }
}

// Blocking operations.
impl<T: TableKv> NamespaceInner<T> {
    /// Pre-build all table unit meta tables.
    fn init_table_unit_meta(&self) -> Result<()> {
        for table_name in self.table_unit_meta_tables() {
            let exists =
                self.table_kv
                    .table_exists(table_name)
                    .box_err()
                    .context(InitTableUnitMeta {
                        namespace: self.name(),
                    })?;
            if !exists {
                self.table_kv
                    .create_table(table_name)
                    .box_err()
                    .context(InitTableUnitMeta {
                        namespace: self.name(),
                    })?;

                info!("Create table unit meta table, table_name:{}", table_name);
            }
        }

        Ok(())
    }

    /// Load all buckets of this namespace.
    fn load_buckets(&self) -> Result<()> {
        let bucket_scan_ctx = self.config.new_bucket_scan_ctx();

        let key_prefix = encoding::bucket_key_prefix(self.name());
        let scan_req = encoding::scan_request_for_prefix(&key_prefix);
        let mut iter = self
            .table_kv
            .scan(bucket_scan_ctx, &self.meta_table_name, scan_req)
            .box_err()
            .context(LoadBuckets {
                namespace: self.name(),
            })?;

        let now = Timestamp::now();
        let mut outdated_buckets = Vec::new();
        while iter.valid() {
            if !iter.key().starts_with(key_prefix.as_bytes()) {
                break;
            }

            let bucket_entry =
                BucketEntry::decode(iter.value())
                    .box_err()
                    .context(LoadBuckets {
                        namespace: self.name(),
                    })?;
            let bucket = Bucket::new(self.name(), bucket_entry)?;

            // Collect the outdated bucket entries for deletion.
            if let Some(ttl) = self.entry.wal.ttl {
                if let Some(earliest) = now.checked_sub_duration(ttl.0) {
                    if bucket_entry.is_expired(earliest) {
                        warn!("Encounter expired bucket entry, skip and collect for later purging here, ttl:{}, expired bucket{:?}", ttl, bucket_entry);
                        outdated_buckets.push(bucket);

                        iter.next().box_err().context(LoadBuckets {
                            namespace: self.name(),
                        })?;

                        continue;
                    }
                }
            }

            // Open the valid bucket here.
            info!(
                "Load bucket for namespace, namespace:{}, bucket:{:?}",
                self.entry.name, bucket_entry
            );

            self.open_bucket(bucket)?;

            iter.next().box_err().context(LoadBuckets {
                namespace: self.name(),
            })?;
        }

        // Try to purge the outdated buckets, unnecessary to wait it.
        let namespace = self.name().to_string();
        let meta_table_name = self.meta_table_name.clone();
        let table_kv = self.table_kv.clone();
        self.runtimes.default_runtime.spawn_blocking(move || {
            let outdated_buckets = outdated_buckets.into_iter().map(Arc::new).collect();
            if let Err(e) = purge_buckets(outdated_buckets, &namespace, &meta_table_name, &table_kv)
            {
                error!(
                    "Try to purge outdated buckets while initializing failed, err:{}",
                    e
                );
            };
        });

        Ok(())
    }

    /// Open bucket, ensure all tables are created, and insert the bucket into
    /// the bucket set in memory.
    fn open_bucket(&self, bucket: Bucket) -> Result<BucketRef> {
        {
            // Create all wal shards of this bucket.
            let mut operator = self.operator.lock().unwrap();
            for wal_shard in &bucket.wal_shard_names {
                operator.create_table_if_needed(&self.table_kv, self.name(), wal_shard)?;
            }
        }

        let bucket = Arc::new(bucket);
        let mut bucket_set = self.bucket_set.write().unwrap();
        bucket_set.insert_bucket(bucket.clone());

        Ok(bucket)
    }

    /// Get bucket by given timestamp, create it if bucket is not exists. The
    /// timestamp will be aligned to bucket duration automatically.
    fn get_or_create_bucket(&self, timestamp: Timestamp) -> Result<BucketRef> {
        let start_ms =
            timestamp
                .checked_floor_by_i64(BUCKET_DURATION_MS)
                .context(BucketOutOfRange {
                    namespace: self.name(),
                    timestamp,
                })?;

        {
            let bucket_set = self.bucket_set.read().unwrap();
            if let Some(bucket) = bucket_set.get_bucket(start_ms) {
                return Ok(bucket.clone());
            }
        }

        // Bucket does not exist, we need to create a new bucket.
        let mut bucket_creator = self.bucket_creator.lock().unwrap();

        bucket_creator.get_or_create_bucket(self, start_ms)
    }

    /// Given timestamp `now` in current time range, create bucket for next time
    /// range.
    fn create_next_bucket(&self, now: Timestamp) -> Result<BucketRef> {
        let now_start = now
            .checked_floor_by_i64(BUCKET_DURATION_MS)
            .context(BucketOutOfRange {
                namespace: self.name(),
                timestamp: now,
            })?;
        let next_start =
            now_start
                .checked_add_i64(BUCKET_DURATION_MS)
                .context(BucketOutOfRange {
                    namespace: self.name(),
                    timestamp: now_start,
                })?;

        let mut bucket_creator = self.bucket_creator.lock().unwrap();

        bucket_creator.get_or_create_bucket(self, next_start)
    }

    /// Purge expired buckets, remove all related wal shard tables and delete
    /// bucket record from meta table.
    fn purge_expired_buckets(&self, now: Timestamp) -> Result<()> {
        if let Some(ttl) = self.entry.wal.ttl {
            // Firstly We should remove expired buckets in memory, because table may have
            // been dropped actually but `drop_table` method returns error.
            let expired_buckets = {
                let mut bucket_set = self.bucket_set.write().unwrap();
                let expired_buckets = bucket_set.expired_buckets(now, ttl.0);

                if expired_buckets.is_empty() {
                    return Ok(());
                }

                for bucket in &expired_buckets {
                    bucket_set.remove_timed_bucket(bucket.entry.gmt_start_ms());
                }

                expired_buckets
            };

            // Then we try our best to remove expired buckets, and the failed ones will be
            // tired to drop again while initializing.
            purge_buckets(
                expired_buckets,
                self.name(),
                &self.meta_table_name,
                &self.table_kv,
            )
            .context(PurgeBucket {
                namespace: self.name(),
                msg: "purge bucket while periodically cleaning",
            })?;
        }

        Ok(())
    }

    fn get_table_unit_from_memory(&self, location: &WalLocation) -> Option<TableUnitRef> {
        let table_units = self.table_units.read().unwrap();
        table_units.get(location).cloned()
    }

    fn insert_or_get_table_unit(
        &self,
        location: WalLocation,
        table_unit: TableUnitRef,
    ) -> TableUnitRef {
        let mut table_units = self.table_units.write().unwrap();
        // Table unit already exists.
        if let Some(v) = table_units.get(&location) {
            return v.clone();
        }

        table_units.insert(location, table_unit.clone());

        table_unit
    }

    fn clean_deleted_logs(&self) -> Result<()> {
        let table_units = self.list_table_units();
        let buckets = self.list_buckets();
        let clean_ctx = self.config.new_clean_ctx();

        for table_unit in table_units {
            table_unit
                .clean_deleted_logs(&self.table_kv, &clean_ctx, &buckets)
                .context(CleanLog {
                    namespace: self.name(),
                    region_id: table_unit.region_id(),
                    table_id: table_unit.table_id(),
                })?;
        }

        Ok(())
    }
}

// Async operations.
impl<T: TableKv> NamespaceInner<T> {
    // FIXME: a dangerous bug, when table are scheduled to another node and
    // scheduled back after, we should deprecate the `TableUnit` entry in memory
    // but now we will continue to use the outdated entry.
    async fn get_or_open_table_unit(&self, location: WalLocation) -> Result<Option<TableUnitRef>> {
        if let Some(table_unit) = self.get_table_unit_from_memory(&location) {
            return Ok(Some(table_unit));
        }

        self.open_table_unit(location).await
    }

    // TODO(yingwen): Provide a close_table_unit() method.
    async fn open_table_unit(&self, location: WalLocation) -> Result<Option<TableUnitRef>> {
        let region_id = location.region_id;
        let table_id = location.table_id;

        let table_unit_meta_table = self.table_unit_meta_table(table_id);
        let buckets = self.bucket_set.read().unwrap().buckets();

        let table_unit_opt = TableUnit::open(
            self.runtimes.clone(),
            &self.table_kv,
            self.config.new_init_scan_ctx(),
            table_unit_meta_table,
            region_id,
            table_id,
            buckets,
        )
        .await
        .context(OpenTableUnit {
            namespace: self.name(),
            region_id,
            table_id,
        })?;
        let table_unit = match table_unit_opt {
            Some(v) => Arc::new(v),
            None => return Ok(None),
        };

        debug!(
            "Open wal table unit, namespace:{}, region_id:{}",
            self.name(),
            region_id
        );

        let table_unit = self.insert_or_get_table_unit(location, table_unit);

        Ok(Some(table_unit))
    }

    // FIXME: a dangerous bug, when table are scheduled to another node and
    // scheduled back after, we should deprecate the `TableUnit` entry in memory
    // but now we will continue to use the outdated entry.
    async fn get_or_create_table_unit(&self, location: WalLocation) -> Result<TableUnitRef> {
        if let Some(table_unit) = self.get_table_unit_from_memory(&location) {
            return Ok(table_unit);
        }

        self.create_table_unit(location).await
    }

    async fn create_table_unit(&self, location: WalLocation) -> Result<TableUnitRef> {
        let table_unit_meta_table = self.table_unit_meta_table(location.table_id);
        let buckets = self.bucket_set.read().unwrap().buckets();

        let table_unit = TableUnit::open_or_create(
            self.runtimes.clone(),
            &self.table_kv,
            self.config.new_init_scan_ctx(),
            table_unit_meta_table,
            location.region_id,
            location.table_id,
            buckets,
        )
        .await
        .context(CreateTableUnit {
            namespace: self.name(),
            location,
        })?;

        debug!(
            "Create wal table unit, namespace:{}, wal location:{:?}",
            self.name(),
            location
        );

        let table_unit = self.insert_or_get_table_unit(location, Arc::new(table_unit));

        Ok(table_unit)
    }

    /// Write log to this namespace.
    async fn write_log(
        &self,
        ctx: &manager::WriteContext,
        batch: &LogWriteBatch,
    ) -> Result<SequenceNumber> {
        trace!(
            "Write batch to namespace:{}, location:{:?}, entries num:{}",
            self.name(),
            batch.location,
            batch.entries.len()
        );

        let now = Timestamp::now();
        // Get current bucket to write.
        let bucket = self.get_or_create_bucket(now)?;

        let table_unit = self.get_or_create_table_unit(batch.location).await?;

        let sequence = table_unit
            .write_log(&self.table_kv, &bucket, ctx, batch)
            .await
            .context(WriteTableUnit {
                namespace: self.name(),
                location: batch.location,
            })?;

        Ok(sequence)
    }

    /// Get last sequence number of this region.
    async fn last_sequence(&self, location: WalLocation) -> Result<SequenceNumber> {
        if let Some(table_unit) = self.get_or_open_table_unit(location).await? {
            return Ok(table_unit.last_sequence());
        }

        Ok(common_types::MIN_SEQUENCE_NUMBER)
    }

    /// Close the region.
    async fn close_region(&self, region_id: RegionId) -> Result<()> {
        let mut table_units = self.table_units.write().unwrap();
        // remote the table unit belongs to this region.
        table_units.retain(|_, v| v.region_id() != region_id);

        Ok(())
    }

    /// Read log from this namespace. Note that the iterating the iterator may
    /// still block caller thread now.
    async fn read_log(&self, ctx: &ReadContext, req: &ReadRequest) -> Result<TableLogIterator<T>> {
        // TODO(yingwen): Skip buckets according to sequence range, avoid scan all
        // buckets.
        let buckets = self.list_buckets();

        if let Some(table_unit) = self.get_or_open_table_unit(req.location).await? {
            table_unit
                .read_log(&self.table_kv, buckets, ctx, req)
                .await
                .context(ReadTableUnit {
                    namespace: self.name(),
                    location: req.location,
                })
        } else {
            Ok(TableLogIterator::new_empty(self.table_kv.clone()))
        }
    }

    /// Delete entries up to `sequence_num` of table unit identified by
    /// `location`.
    async fn delete_entries(
        &self,
        location: WalLocation,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        if let Some(table_unit) = self.get_or_open_table_unit(location).await? {
            let table_unit_meta_table = self.table_unit_meta_table(location.table_id);

            table_unit
                .delete_entries_up_to(&self.table_kv, table_unit_meta_table, sequence_num)
                .await
                .context(DeleteEntries {
                    namespace: self.name(),
                    location,
                })?;
        }

        Ok(())
    }

    pub async fn scan_log(
        &self,
        ctx: &ScanContext,
        request: &ScanRequest,
    ) -> Result<TableLogIterator<T>> {
        // Prepare start/end sequence to read, now this doesn't provide snapshot
        // isolation semantics since delete and write operations may happen
        // during reading start/end sequence.
        let buckets = self.list_buckets();

        let region_id = request.region_id;
        let min_log_key = CommonLogKey::new(region_id, TableId::MIN, SequenceNumber::MIN);
        let max_log_key = CommonLogKey::new(region_id, TableId::MAX, SequenceNumber::MAX);

        let scan_ctx = KvScanContext {
            timeout: ctx.timeout,
            ..Default::default()
        };

        Ok(TableLogIterator::new(
            buckets,
            min_log_key,
            max_log_key,
            scan_ctx,
            self.table_kv.clone(),
        ))
    }
}

/// BucketCreator handles bucket creation and persistence.
struct BucketCreator;

impl BucketCreator {
    /// Get bucket by given timestamp `start_ms`, create it if bucket is not
    /// exists. The caller should ensure the timestamp is aligned to bucket
    /// duration.
    fn get_or_create_bucket<T: TableKv>(
        &mut self,
        inner: &NamespaceInner<T>,
        start_ms: Timestamp,
    ) -> Result<BucketRef> {
        {
            let bucket_set = inner.bucket_set.read().unwrap();
            if let Some(bucket) = bucket_set.get_bucket(start_ms) {
                return Ok(bucket.clone());
            }
        }

        let bucket_entry = if inner.config.ttl.is_some() {
            // Bucket with ttl.
            BucketEntry::new_timed(inner.entry.wal.shard_num, start_ms, BUCKET_DURATION_MS)
                .context(BucketOutOfRange {
                    namespace: inner.name(),
                    timestamp: start_ms,
                })?
        } else {
            // Permanent bucket.
            BucketEntry::new_permanent(inner.entry.wal.shard_num)
        };

        info!(
            "Try to create bucket, namespace:{}, start_ms:{:?}, bucket:{:?}",
            inner.name(),
            start_ms,
            bucket_entry
        );

        assert!(
            bucket_entry.is_permanent() == inner.config.ttl.is_none(),
            "Bucket should be consistent with ttl config, bucket:{:?}, ttl:{:?}",
            bucket_entry,
            inner.config.ttl,
        );

        let bucket = Bucket::new(inner.name(), bucket_entry)?;

        self.create_bucket(inner, bucket)
    }

    /// Create and open the bucket.
    fn create_bucket<T: TableKv>(
        &mut self,
        inner: &NamespaceInner<T>,
        bucket: Bucket,
    ) -> Result<BucketRef> {
        // Insert bucket record into TableKv.
        let bucket = self.try_persist_bucket(inner, bucket)?;

        inner.open_bucket(bucket)
    }

    /// Try to persist and return the persisted bucket, if bucket already
    /// exists, return the bucket from storage.
    fn try_persist_bucket<T: TableKv>(
        &mut self,
        inner: &NamespaceInner<T>,
        bucket: Bucket,
    ) -> Result<Bucket> {
        // Insert bucket record into TableKv.
        let key = bucket.format_bucket_key(inner.name())?;
        let value = bucket.entry.encode().context(Encode {
            namespace: inner.name(),
        })?;

        info!(
            "Persist bucket entry, namespace:{}, bucket:{:?}",
            inner.name(),
            bucket.entry
        );

        let mut batch = T::WriteBatch::default();
        batch.insert(key.as_bytes(), &value);

        let res = inner
            .table_kv
            .write(WriteContext::default(), &inner.meta_table_name, batch);
        if let Err(e) = &res {
            if e.is_primary_key_duplicate() {
                info!(
                    "Bucket already persisted, namespace:{}, bucket:{:?}",
                    inner.name(),
                    bucket.entry
                );

                // Load given bucket entry from storage.
                let bucket = self.get_bucket_by_key(inner, &key)?;

                info!(
                    "Load bucket from storage, namespace:{}, bucket:{:?}",
                    inner.name(),
                    bucket.entry
                );

                return Ok(bucket);
            } else {
                error!("Failed to persist bucket, key:{}, err:{}", key, e);

                res.box_err().context(PersistValue { key })?;
            }
        }

        Ok(bucket)
    }

    fn get_bucket_by_key<T: TableKv>(
        &self,
        inner: &NamespaceInner<T>,
        key: &str,
    ) -> Result<Bucket> {
        let value = get_value(&inner.table_kv, &inner.meta_table_name, key)?;
        let bucket_entry = BucketEntry::decode(&value).context(Decode { key })?;

        let bucket = Bucket::new(inner.name(), bucket_entry)?;

        Ok(bucket)
    }
}

fn get_value<T: TableKv>(table_kv: &T, meta_table_name: &str, key: &str) -> Result<Vec<u8>> {
    table_kv
        .get(meta_table_name, key.as_bytes())
        .box_err()
        .context(GetValue { key })?
        .context(ValueNotFound { key })
}

fn get_namespace_entry_by_key<T: TableKv>(
    table_kv: &T,
    meta_table_name: &str,
    key: &str,
) -> Result<NamespaceEntry> {
    let value = get_value(table_kv, meta_table_name, key)?;
    let namespace_entry = NamespaceEntry::decode(&value).context(Decode { key })?;

    Ok(namespace_entry)
}

pub struct Namespace<T> {
    inner: Arc<NamespaceInner<T>>,
    monitor_handle: Option<TaskHandle>,
    cleaner_handle: Option<TaskHandle>,
}

impl<T> fmt::Debug for Namespace<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let table_unit_num = self.inner.table_units.read().unwrap().len();

        f.debug_struct("Namespace")
            .field("entry", &self.inner.entry)
            .field("bucket_set", &self.inner.bucket_set)
            .field("table_units", &table_unit_num)
            .field("meta_table_name", &self.inner.meta_table_name)
            .field("table_unit_meta_tables", &self.inner.table_unit_meta_tables)
            .field("config", &self.inner.config)
            .finish()
    }
}

impl<T> Namespace<T> {
    /// Name of the namespace.
    #[inline]
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    #[inline]
    pub fn read_runtime(&self) -> &Arc<Runtime> {
        &self.inner.runtimes.read_runtime
    }
}

// Blocking operations
impl<T: TableKv> Namespace<T> {
    pub fn open(
        table_kv: &T,
        runtimes: WalRuntimes,
        name: &str,
        mut config: NamespaceConfig,
    ) -> Result<Self> {
        config.sanitize();

        Self::init_meta_table(table_kv, consts::META_TABLE_NAME)?;

        let namespace =
            match Self::load_namespace_from_meta(table_kv, consts::META_TABLE_NAME, name)? {
                Some(namespace_entry) => {
                    assert!(
                    namespace_entry.wal.enable_ttl == config.ttl.is_some(),
                    "It's impossible to be different because the it can't be set by user actually, 
                        but now the original ttl set is:{}, current in config is:{}",
                    namespace_entry.wal.enable_ttl,
                    config.ttl.is_some()
                );

                    Namespace::new(
                        runtimes,
                        table_kv.clone(),
                        consts::META_TABLE_NAME,
                        namespace_entry,
                        config,
                    )?
                }

                None => Namespace::try_persist_namespace(
                    runtimes,
                    table_kv,
                    consts::META_TABLE_NAME,
                    name,
                    config,
                )?,
            };

        Ok(namespace)
    }

    /// Returns true if we ensure the table is already created.
    fn is_table_created(table_kv: &T, table_name: &str) -> bool {
        match table_kv.table_exists(table_name) {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to check table existence, err:{}", e);

                false
            }
        }
    }

    fn init_meta_table(table_kv: &T, meta_table_name: &str) -> Result<()> {
        if !Self::is_table_created(table_kv, meta_table_name) {
            info!("Try to create meta table, table_name:{}", meta_table_name);

            table_kv
                .create_table(meta_table_name)
                .box_err()
                .context(CreateTable)?;

            info!(
                "Create meta table successfully, table_name:{}",
                meta_table_name
            );
        }

        Ok(())
    }

    fn load_namespace_from_meta(
        table_kv: &T,
        meta_table_name: &str,
        namespace_name: &str,
    ) -> Result<Option<NamespaceEntry>> {
        let key = encoding::format_namespace_key(namespace_name);
        let value_opt = table_kv
            .get(meta_table_name, key.as_bytes())
            .box_err()
            .context(GetValue { key: &key })?;

        match value_opt {
            Some(value) => {
                let namespace_entry = NamespaceEntry::decode(&value).context(Decode { key })?;
                Ok(Some(namespace_entry))
            }
            None => Ok(None),
        }
    }

    fn new(
        runtimes: WalRuntimes,
        table_kv: T,
        meta_table_name: &str,
        entry: NamespaceEntry,
        config: NamespaceConfig,
    ) -> Result<Self> {
        let mut table_unit_meta_tables = Vec::with_capacity(entry.table_unit_meta.shard_num);
        for shard_id in 0..entry.table_unit_meta.shard_num {
            let table_name = encoding::format_table_unit_meta_name(&entry.name, shard_id);
            table_unit_meta_tables.push(table_name);
        }

        let bucket_set = BucketSet::new(config.ttl.is_some());

        let inner = Arc::new(NamespaceInner {
            runtimes: runtimes.clone(),
            table_kv,
            entry,
            bucket_set: RwLock::new(bucket_set),
            table_units: RwLock::new(HashMap::new()),
            meta_table_name: meta_table_name.to_string(),
            table_unit_meta_tables,
            operator: Mutex::new(TableOperator),
            bucket_creator: Mutex::new(BucketCreator),
            config,
        });

        inner.init_table_unit_meta()?;

        inner.load_buckets()?;

        let (mut monitor_handle, mut cleaner_handle) = (None, None);
        if inner.entry.wal.ttl.is_some() {
            info!("Start bucket monitor, namespace:{}", inner.name());

            // Has ttl, we need to periodically create/purge buckets.
            monitor_handle = Some(start_bucket_monitor(
                &runtimes.default_runtime,
                BUCKET_MONITOR_PERIOD,
                inner.clone(),
            ));
        } else {
            info!("Start log cleaner, namespace:{}", inner.name());

            // Start a cleaner if wal has no ttl.
            cleaner_handle = Some(start_log_cleaner(
                &runtimes.default_runtime,
                LOG_CLEANER_PERIOD,
                inner.clone(),
            ));
        }

        let namespace = Self {
            inner,
            monitor_handle,
            cleaner_handle,
        };

        Ok(namespace)
    }

    /// Try to persist the namespace, if the namespace already exists, returns
    /// the existing namespace.
    fn try_persist_namespace(
        runtimes: WalRuntimes,
        table_kv: &T,
        meta_table_name: &str,
        namespace: &str,
        config: NamespaceConfig,
    ) -> Result<Namespace<T>> {
        let mut namespace_entry = config
            .new_namespace_entry(namespace)
            .context(BuildNamespace { namespace })?;

        let key = encoding::format_namespace_key(namespace);
        let value = namespace_entry.encode().context(Encode { namespace })?;

        let mut batch = T::WriteBatch::default();
        batch.insert(key.as_bytes(), &value);

        // Try to persist namespace entry.
        let res = table_kv.write(WriteContext::default(), meta_table_name, batch);
        if let Err(e) = &res {
            if e.is_primary_key_duplicate() {
                // Another client has already persisted the namespace.
                info!(
                    "Namespace already persisted, key:{}, config:{:?}",
                    key, config
                );

                // Load given namespace from storage.
                namespace_entry = get_namespace_entry_by_key(table_kv, meta_table_name, &key)?;

                info!(
                    "Load namespace from storage, key:{}, entry:{:?}",
                    key, namespace_entry
                );
            } else {
                error!("Failed to persist namespace, key:{}, err:{}", key, e);

                res.box_err().context(PersistValue { key })?;
            }
        }

        Namespace::new(
            runtimes,
            table_kv.clone(),
            meta_table_name,
            namespace_entry,
            config,
        )
    }

    pub fn get_statistics(&self) -> String {
        self.inner.get_statistics()
    }
}

// Async operations.
impl<T: TableKv> Namespace<T> {
    /// Write log to this namespace.
    pub async fn write_log(
        &self,
        ctx: &manager::WriteContext,
        batch: &LogWriteBatch,
    ) -> Result<SequenceNumber> {
        self.inner.write_log(ctx, batch).await
    }

    /// Get last sequence number of this table unit.
    pub async fn last_sequence(&self, location: WalLocation) -> Result<SequenceNumber> {
        self.inner.last_sequence(location).await
    }

    pub async fn close_region(&self, region_id: RegionId) -> Result<()> {
        self.inner.close_region(region_id).await
    }

    /// Read log from this namespace. Note that the iterating the iterator may
    /// still block caller thread now.
    pub async fn read_log(
        &self,
        ctx: &ReadContext,
        req: &ReadRequest,
    ) -> Result<TableLogIterator<T>> {
        self.inner.read_log(ctx, req).await
    }

    /// Delete entries up to `sequence_num` of table unit identified by
    /// `region_id` and `table_id`.
    pub async fn delete_entries(
        &self,
        location: WalLocation,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        self.inner.delete_entries(location, sequence_num).await
    }

    /// Scan logs of a whole region from this namespace.
    // TODO: maybe we should filter the log marked deleted,
    // but there isn't any actual benefit such as reducing network IO,
    // so it seems not so important.
    pub async fn scan_log(
        &self,
        ctx: &ScanContext,
        req: &ScanRequest,
    ) -> Result<TableLogIterator<T>> {
        self.inner.scan_log(ctx, req).await
    }

    /// Stop background tasks and close this namespace.
    pub async fn close(&self) -> Result<()> {
        info!("Try to close namespace, namespace:{}", self.name());

        self.inner.clear_table_units();

        if let Some(monitor_handle) = &self.monitor_handle {
            monitor_handle.stop_task().await.context(StopTask {
                namespace: self.name(),
            })?;
        }

        if let Some(cleaner_handle) = &self.cleaner_handle {
            cleaner_handle.stop_task().await.context(StopTask {
                namespace: self.name(),
            })?;
        }

        info!("Namespace closed, namespace:{}", self.name());

        Ok(())
    }
}

pub type NamespaceRef<T> = Arc<Namespace<T>>;

/// Table operator wraps create/drop table operations.
struct TableOperator;

impl TableOperator {
    fn create_table_if_needed<T: TableKv>(
        &mut self,
        table_kv: &T,
        namespace: &str,
        table_name: &str,
    ) -> Result<()> {
        let table_exists = table_kv
            .table_exists(table_name)
            .box_err()
            .context(BucketMeta { namespace })?;
        if !table_exists {
            table_kv
                .create_table(table_name)
                .box_err()
                .context(BucketMeta { namespace })?;
        }

        Ok(())
    }
}

/// Time buckets of a namespace, orderded by start time.
#[derive(Debug)]
pub enum BucketSet {
    Timed(BTreeMap<Timestamp, BucketRef>),
    Permanent(Option<BucketRef>),
}

impl BucketSet {
    fn new(has_ttl: bool) -> Self {
        if has_ttl {
            BucketSet::Timed(BTreeMap::new())
        } else {
            BucketSet::Permanent(None)
        }
    }

    fn insert_bucket(&mut self, bucket: BucketRef) {
        let old_bucket = match self {
            BucketSet::Timed(buckets) => {
                buckets.insert(bucket.entry.gmt_start_ms(), bucket.clone())
            }
            BucketSet::Permanent(old_bucket) => old_bucket.replace(bucket.clone()),
        };

        assert!(
            old_bucket.is_none(),
            "Try to overwrite old bucket, old_bucket:{old_bucket:?}, new_bucket:{bucket:?}",
        );
    }

    /// Get bucket by start time. The caller need to ensure the timestamp is
    /// aligned to the bucket duration.
    fn get_bucket(&self, start_ms: Timestamp) -> Option<&BucketRef> {
        match self {
            BucketSet::Timed(buckets) => buckets.get(&start_ms),
            BucketSet::Permanent(bucket) => bucket.as_ref(),
        }
    }

    fn expired_buckets(&self, now: Timestamp, ttl: Duration) -> Vec<BucketRef> {
        match self {
            BucketSet::Timed(all_buckets) => {
                let mut buckets = Vec::new();
                if let Some(earliest) = now.checked_sub_duration(ttl) {
                    for (_ts, bucket) in all_buckets.range(..=earliest) {
                        if bucket.entry.is_expired(earliest) {
                            buckets.push(bucket.clone());
                        }
                    }
                }

                buckets
            }
            BucketSet::Permanent(_) => Vec::new(),
        }
    }

    fn buckets(&self) -> Vec<BucketRef> {
        match self {
            BucketSet::Timed(buckets) => buckets.values().cloned().collect(),
            BucketSet::Permanent(bucket) => match bucket {
                Some(b) => vec![b.clone()],
                None => Vec::new(),
            },
        }
    }

    /// Remove timed bucket, does nothing if this is a permanent bucket set.
    fn remove_timed_bucket(&mut self, start_ms: Timestamp) {
        if let BucketSet::Timed(buckets) = self {
            buckets.remove(&start_ms);
        }
    }
}

#[derive(Debug, Clone)]
pub struct Bucket {
    entry: BucketEntry,
    wal_shard_names: Vec<String>,
}

impl Bucket {
    fn new(namespace: &str, entry: BucketEntry) -> Result<Self> {
        let mut wal_shard_names = Vec::with_capacity(entry.shard_num);

        for shard_id in 0..entry.shard_num {
            let table_name = if entry.is_permanent() {
                encoding::format_permanent_wal_name(namespace, shard_id)
            } else {
                encoding::format_timed_wal_name(namespace, entry.gmt_start_ms(), shard_id)
                    .map_err(|e| Box::new(e) as _)
                    .context(BucketMeta { namespace })?
            };

            wal_shard_names.push(table_name);
        }

        Ok(Self {
            entry,
            wal_shard_names,
        })
    }

    #[inline]
    pub fn gmt_start_ms(&self) -> Timestamp {
        self.entry.gmt_start_ms()
    }

    #[inline]
    pub fn wal_shard_table(&self, region_id: u64) -> &str {
        let index = region_id as usize % self.wal_shard_names.len();
        &self.wal_shard_names[index]
    }

    fn format_bucket_key(&self, namespace: &str) -> Result<String> {
        match self.entry.bucket_duration() {
            Some(bucket_duration) => {
                // Timed bucket.
                encoding::format_timed_bucket_key(
                    namespace,
                    ReadableDuration(bucket_duration),
                    self.entry.gmt_start_ms(),
                )
                .map_err(|e| Box::new(e) as _)
                .context(BucketMeta { namespace })
            }
            None => {
                // This is a permanent bucket.
                Ok(encoding::format_permanent_bucket_key(namespace))
            }
        }
    }
}

pub type BucketRef = Arc<Bucket>;

async fn log_cleaner_routine<T: TableKv>(inner: Arc<NamespaceInner<T>>) {
    debug!(
        "Periodical log cleaning process start, namespace:{}",
        inner.name(),
    );

    if let Err(e) = inner.clean_deleted_logs() {
        error!(
            "Failed to clean deleted logs, namespace:{}, err:{}",
            inner.name(),
            e,
        );
    }

    debug!(
        "Periodical log cleaning process end, namespace:{}",
        inner.name()
    );
}

fn start_log_cleaner<T: TableKv>(
    runtime: &Runtime,
    period: Duration,
    namespace: Arc<NamespaceInner<T>>,
) -> TaskHandle {
    let name = format!("LogCleaner-{}", namespace.name());
    let builder = move || {
        let inner = namespace.clone();

        log_cleaner_routine(inner)
    };

    TimedTask::start_timed_task(name, runtime, period, builder)
}

async fn bucket_monitor_routine<T: TableKv>(inner: Arc<NamespaceInner<T>>, now: Timestamp) {
    debug!(
        "Periodical bucket monitor process start, namespace:{}, now:{:?}.",
        inner.name(),
        now
    );

    // Now failure of one namespace won't abort the whole manage procedure.
    if let Err(e) = inner.create_next_bucket(now) {
        error!(
            "Failed to create next bucket, namespace:{}, now:{:?}, err:{}",
            inner.name(),
            now,
            e,
        );
    }

    if let Err(e) = inner.purge_expired_buckets(now) {
        error!(
            "Failed to purge expired buckets, namespace:{}, now:{:?}, err:{}",
            inner.name(),
            now,
            e,
        );
    }

    debug!(
        "Periodical bucket monitor process end, namespace:{}",
        inner.name()
    );
}

fn start_bucket_monitor<T: TableKv>(
    runtime: &Runtime,
    period: Duration,
    namespace: Arc<NamespaceInner<T>>,
) -> TaskHandle {
    let name = format!("BucketMonitor-{}", namespace.name());
    let builder = move || {
        let inner = namespace.clone();
        let now = Timestamp::now();

        bucket_monitor_routine(inner, now)
    };

    TimedTask::start_timed_task(name, runtime, period, builder)
}

fn purge_buckets<T: TableKv>(
    buckets: Vec<BucketRef>,
    namespace: &str,
    meta_table_name: &str,
    table_kv: &T,
) -> std::result::Result<(), GenericError> {
    let mut batch = T::WriteBatch::with_capacity(buckets.len());
    let mut keys = Vec::with_capacity(buckets.len());
    for bucket in &buckets {
        // Delete all tables of this bucket.
        for table_name in &bucket.wal_shard_names {
            let _ = table_kv.drop_table(table_name).map_err(|e| {
                error!("Purge buckets drop table failed, table:{table_name}, err:{e}");
            });
        }

        // All tables of this bucket have been dropped, we can remove the bucket record
        // later.
        let key = bucket.format_bucket_key(namespace).map_err(Box::new)?;

        batch.delete(key.as_bytes());

        keys.push(key);
    }

    // Delete the bucket entry in meta table.
    table_kv
        .write(WriteContext::default(), meta_table_name, batch)
        .map_err(Box::new)?;

    info!("Purge expired buckets, keys:{:?}", keys);

    Ok(())
}