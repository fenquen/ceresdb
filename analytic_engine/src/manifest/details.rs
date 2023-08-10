// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Implementation of Manifest

use std::{
    collections::VecDeque,
    fmt, mem,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use ceresdbproto::manifest as manifest_pb;
use generic_error::{BoxError, GenericError, GenericResult};
use log::{debug, info, warn};
use macros::define_result;
use object_store::{ObjectStoreRef, Path};
use parquet::data_type::AsBytes;
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, ResultExt, Snafu};
use table_engine::table::TableId;
use time_ext::ReadableDuration;
use tokio::sync::Mutex;
use wal::{
    kv_encoder::LogBatchEncoder,
    log_batch::LogEntry,
    manager::{
        BatchLogIteratorAdapter, ReadBoundary, ReadContext, ReadRequest, SequenceNumber,
        WalLocation, WalManagerRef, WriteContext,
    },
};
use wal::manager::WalManager;

use crate::{
    manifest::{
        meta_edit::{
            MetaEdit, MetaEditRequest, MetaUpdate, MetaUpdateDecoder, MetaUpdatePayload, Snapshot,
        },
        meta_snapshot::{MetaSnapshot, MetaSnapshotBuilder},
        LoadRequest, Manifest, SnapshotRequest,
    },
    space::SpaceId,
    table::data::{TableDataRef, TableShardInfo},
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub (crate)))]
pub enum Error {
    #[snafu(display(
    "Failed to encode payloads, wal_location:{:?}, err:{}",
    wal_location,
    source
    ))]
    EncodePayloads {
        wal_location: WalLocation,
        source: wal::manager::Error,
    },

    #[snafu(display("Failed to write update to wal, err:{}", source))]
    WriteWal { source: wal::manager::Error },

    #[snafu(display("Failed to read wal, err:{}", source))]
    ReadWal { source: wal::manager::Error },

    #[snafu(display("Failed to read log entry, err:{}", source))]
    ReadEntry { source: wal::manager::Error },

    #[snafu(display("Failed to apply table meta update, err:{}", source))]
    ApplyUpdate {
        source: crate::manifest::meta_snapshot::Error,
    },

    #[snafu(display("Failed to clean wal, err:{}", source))]
    CleanWal { source: wal::manager::Error },

    #[snafu(display(
    "Failed to store snapshot, err:{}.\nBacktrace:\n{:?}",
    source,
    backtrace
    ))]
    StoreSnapshot {
        source: object_store::ObjectStoreError,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Failed to fetch snapshot, err:{}.\nBacktrace:\n{:?}",
    source,
    backtrace
    ))]
    FetchSnapshot {
        source: object_store::ObjectStoreError,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Failed to decode snapshot, err:{}.\nBacktrace:\n{:?}",
    source,
    backtrace
    ))]
    DecodeSnapshot {
        source: prost::DecodeError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to build snapshot, msg:{}.\nBacktrace:\n{:?}", msg, backtrace))]
    BuildSnapshotNoCause { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to build snapshot, msg:{}, err:{}", msg, source))]
    BuildSnapshotWithCause { msg: String, source: GenericError },

    #[snafu(display(
    "Failed to apply edit to table, msg:{}.\nBacktrace:\n{:?}",
    msg,
    backtrace
    ))]
    ApplyUpdateToTableNoCause { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to apply edit to table, msg:{}, err:{}", msg, source))]
    ApplyUpdateToTableWithCause { msg: String, source: GenericError },

    #[snafu(display(
    "Failed to apply snapshot to table, msg:{}.\nBacktrace:\n{:?}",
    msg,
    backtrace
    ))]
    ApplySnapshotToTableNoCause { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to apply snapshot to table, msg:{}, err:{}", msg, source))]
    ApplySnapshotToTableWithCause { msg: String, source: GenericError },

    #[snafu(display("Failed to load snapshot, err:{}", source))]
    LoadSnapshot { source: GenericError },
}

define_result!(Error);

#[async_trait]
trait MetaUpdateLogEntryIterator {
    async fn next_update(&mut self) -> Result<Option<(SequenceNumber, MetaUpdate)>>;
}

/// Implementation of [`MetaUpdateLogEntryIterator`].
#[derive(Debug)]
pub struct MetaUpdateReaderImpl {
    iterator: BatchLogIteratorAdapter,
    has_next: bool,
    buffer: VecDeque<LogEntry<MetaUpdate>>,
}

#[async_trait]
impl MetaUpdateLogEntryIterator for MetaUpdateReaderImpl {
    async fn next_update(&mut self) -> Result<Option<(SequenceNumber, MetaUpdate)>> {
        if !self.has_next {
            return Ok(None);
        }

        if self.buffer.is_empty() {
            let decoder = MetaUpdateDecoder;
            let buffer = mem::take(&mut self.buffer);
            self.buffer = self.iterator.next_log_entries(decoder, buffer).await.context(ReadEntry)?;
        }

        match self.buffer.pop_front() {
            Some(logEntry) => Ok(Some((logEntry.sequence, logEntry.payload))),
            None => {
                self.has_next = false;
                Ok(None)
            }
        }
    }
}

/// Table meta set
///
/// Get snapshot of or modify table's metadata through it.
pub(crate) trait TableMetaSet: fmt::Debug + Send + Sync {
    // Get snapshot of `TableData`.
    fn get_table_snapshot(&self,
                          space_id: SpaceId,
                          table_id: TableId) -> Result<Option<MetaSnapshot>>;

    // Apply update to `TableData` and return it.
    fn apply_edit_to_table(&self, metaEditRequest: MetaEditRequest) -> Result<TableDataRef>;
}

/// Snapshot recoverer
///
/// Usually, it will recover the snapshot from storage(like disk, oss, etc).
// TODO: remove `LogStore` and related operations, it should be called directly but not in the `SnapshotReoverer`.
#[derive(Debug, Clone)]
struct SnapshotRecoverer<MetaUpdateLogStore_, MetaUpdateSnapshotStore_> {
    table_id: TableId,
    space_id: SpaceId,
    metaUpdateLogStore: MetaUpdateLogStore_,
    metaUpdateSnapshotStore: MetaUpdateSnapshotStore_,
}

impl<LogStore, SnapshotStore> SnapshotRecoverer<LogStore, SnapshotStore> where LogStore: MetaUpdateLogStore + Send + Sync,
                                                                               SnapshotStore: MetaUpdateSnapshotStore + Send + Sync {
    async fn recover(&self) -> Result<Option<Snapshot>> {
        // Load the current snapshot first.
        match self.metaUpdateSnapshotStore.load().await? {
            Some(snapshot) => Ok(Some(self.create_latest_snapshot_with_prev(snapshot).await?)),
            None => self.createLatestSnapshotWithoutPrev().await,
        }
    }

    async fn create_latest_snapshot_with_prev(&self, prev_snapshot: Snapshot) -> Result<Snapshot> {
        debug!("manifest recover with prev snapshot, snapshot:{:?}, table_id:{}, space_id:{}",
            prev_snapshot, self.table_id, self.space_id);

        let log_start_boundary = ReadBoundary::Excluded(prev_snapshot.end_seq);
        let mut reader = self.metaUpdateLogStore.scan(log_start_boundary).await?;

        let mut latest_seq = prev_snapshot.end_seq;
        let mut manifest_data_builder = if let Some(v) = prev_snapshot.data {
            MetaSnapshotBuilder::new(Some(v.table_meta), v.version_meta)
        } else {
            MetaSnapshotBuilder::default()
        };
        while let Some((seq, update)) = reader.next_update().await? {
            latest_seq = seq;
            manifest_data_builder
                .apply_update(update)
                .context(ApplyUpdate)?;
        }
        Ok(Snapshot {
            end_seq: latest_seq,
            data: manifest_data_builder.build(),
        })
    }

    async fn createLatestSnapshotWithoutPrev(&self) -> Result<Option<Snapshot>> {
        debug!("manifest recover without prev snapshot, table_id:{}, space_id:{}",self.table_id, self.space_id);

        let mut iter = self.metaUpdateLogStore.scan(ReadBoundary::Min).await?;

        let mut latest_seq = SequenceNumber::MIN;
        let mut manifest_data_builder = MetaSnapshotBuilder::default();
        let mut has_logs = false;

        while let Some((seq, metaUpdate)) = iter.next_update().await? {
            latest_seq = seq;
            manifest_data_builder.apply_update(metaUpdate).context(ApplyUpdate)?;
            has_logs = true;
        }

        if has_logs {
            Ok(Some(Snapshot {
                end_seq: latest_seq,
                data: manifest_data_builder.build(),
            }))
        } else {
            info!("manifest recover nothing, table_id:{}, space_id:{}",self.table_id, self.space_id);
            Ok(None)
        }
    }
}

/// Snapshot creator
///
/// Usually, it will get snapshot from memory, and store them to storage(like
/// disk, oss, etc).
// TODO: remove `LogStore` and related operations, it should be called directly but not in the
// `Snapshotter`.
#[derive(Debug, Clone)]
struct Snapshotter<LogStore, SnapshotStore> {
    log_store: LogStore,
    snapshot_store: SnapshotStore,
    end_seq: SequenceNumber,
    snapshot_data_provider: Arc<dyn TableMetaSet>,
    space_id: SpaceId,
    table_id: TableId,
}

impl<LogStore, SnapshotStore> Snapshotter<LogStore, SnapshotStore> where LogStore: MetaUpdateLogStore + Send + Sync,
                                                                         SnapshotStore: MetaUpdateSnapshotStore + Send + Sync {
    /// Create a latest snapshot of the current logs.
    async fn snapshot(&self) -> Result<Option<Snapshot>> {
        // Get snapshot data from memory.
        let table_snapshot_opt = self.snapshot_data_provider.get_table_snapshot(self.space_id, self.table_id)?;

        let snapshot = Snapshot {
            end_seq: self.end_seq,
            data: table_snapshot_opt,
        };

        // Update the current snapshot to the new one.
        self.snapshot_store.store(&snapshot).await?;

        // Delete the expired logs after saving the snapshot.
        // TODO: Actually this operation can be performed background, and the failure of it can be ignored.
        self.log_store.delete_up_to(snapshot.end_seq).await?;

        Ok(Some(snapshot))
    }
}

/// Options for manifest
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Options {
    /// Steps to do snapshot
    // TODO: move this field to suitable place.
    pub snapshot_every_n_updates: NonZeroUsize,

    /// Timeout to read manifest entries
    pub scan_timeout: ReadableDuration,

    /// Batch size to read manifest entries
    // TODO: use NonZeroUsize
    pub scan_batch_size: usize,

    /// Timeout to store manifest entries
    pub store_timeout: ReadableDuration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            snapshot_every_n_updates: NonZeroUsize::new(100).unwrap(),
            scan_timeout: ReadableDuration::secs(5),
            scan_batch_size: 100,
            store_timeout: ReadableDuration::secs(5),
        }
    }
}

/// The implementation based on wal and object store of [`Manifest`].
#[derive(Debug)]
pub struct ManifestImpl {
    opts: Options,
    wal_manager: WalManagerRef,
    store: ObjectStoreRef,

    /// Number of updates wrote to wal since last snapshot.
    num_updates_since_snapshot: Arc<AtomicUsize>,

    /// Ensure the snapshot procedure is non-concurrent.
    ///
    /// Use tokio mutex because this guard protects the snapshot procedure which
    /// contains io operations.
    snapshot_write_guard: Arc<Mutex<()>>,

    tableMetaSet: Arc<dyn TableMetaSet>,
}

impl ManifestImpl {
    pub(crate) async fn open(
        opts: Options,
        wal_manager: WalManagerRef,
        store: ObjectStoreRef,
        table_meta_set: Arc<dyn TableMetaSet>,
    ) -> Result<Self> {
        let manifest = Self {
            opts,
            wal_manager,
            store,
            num_updates_since_snapshot: Arc::new(AtomicUsize::new(0)),
            snapshot_write_guard: Arc::new(Mutex::new(())),
            tableMetaSet: table_meta_set,
        };

        Ok(manifest)
    }

    async fn store_update_to_wal(&self,
                                 metaUpdate: MetaUpdate,
                                 walLocation: WalLocation) -> Result<SequenceNumber> {
        let metaUpdateLogStoreWalBased = MetaUpdateLogStoreWalBased {
            opts: self.opts.clone(),
            location: walLocation,
            walManager: self.wal_manager.clone(),
        };
        let latest_sequence = metaUpdateLogStoreWalBased.append(metaUpdate).await?;
        self.num_updates_since_snapshot.fetch_add(1, Ordering::Relaxed);

        Ok(latest_sequence)
    }

    /// Do snapshot if no other snapshot is triggered.
    ///
    /// Returns the latest snapshot if snapshot is done.
    async fn do_snapshot_internal(&self,
                                  space_id: SpaceId,
                                  table_id: TableId,
                                  location: WalLocation, ) -> Result<Option<Snapshot>> {
        if let Ok(_guard) = self.snapshot_write_guard.try_lock() {
            let log_store = MetaUpdateLogStoreWalBased {
                opts: self.opts.clone(),
                location,
                walManager: self.wal_manager.clone(),
            };
            let snapshot_store =
                MetaUpdateSnapshotStoreObjectStoreBased::new(space_id, table_id, self.store.clone());
            let end_seq = self.wal_manager.sequence_num(location).await.unwrap();
            let snapshotter = Snapshotter {
                log_store,
                snapshot_store,
                end_seq,
                snapshot_data_provider: self.tableMetaSet.clone(),
                space_id,
                table_id,
            };

            let snapshot = snapshotter.snapshot().await?;
            Ok(snapshot)
        } else {
            debug!("avoid concurrent snapshot");
            Ok(None)
        }
    }
}

#[async_trait]
impl Manifest for ManifestImpl {
    async fn apply_edit(&self, metaEditRequest: MetaEditRequest) -> GenericResult<()> {
        info!("manifest store update, request:{:?}", metaEditRequest);

        let MetaEditRequest {
            tableShardInfo: shard_info,
            metaEdit: meta_edit,
        } = metaEditRequest.clone();

        let meta_update = MetaUpdate::try_from(meta_edit).box_err()?;
        let table_id = meta_update.table_id();
        let shard_id = shard_info.shard_id;
        let walLocation = WalLocation::new(shard_id as u64, table_id.as_u64());
        let space_id = meta_update.space_id();

        // 落实到wal算是硬盘上的
        self.store_update_to_wal(meta_update, walLocation).await?;

        // 内存上的update
        let table_data = self.tableMetaSet.apply_edit_to_table(metaEditRequest).box_err()?;

        // Update manifest updates count.
        table_data.increase_manifest_updates(1);

        // Judge if snapshot is needed.
        if table_data.should_do_manifest_snapshot() {
            self.do_snapshot_internal(space_id, table_id, walLocation).await?;
            table_data.reset_manifest_updates();
        }

        Ok(())
    }

    async fn recover(&self, load_req: &LoadRequest) -> GenericResult<()> {
        info!("Manifest recover begin, request:{load_req:?}");

        // Load table meta snapshot from storage.
        let walLocation = WalLocation::new(load_req.shard_id as u64, load_req.table_id.as_u64());

        let log_store = MetaUpdateLogStoreWalBased {
            opts: self.opts.clone(),
            location: walLocation,
            walManager: self.wal_manager.clone(),
        };

        let snapshot_store = MetaUpdateSnapshotStoreObjectStoreBased::new(
            load_req.space_id,
            load_req.table_id,
            self.store.clone(),
        );

        let snapshotRecoverer = SnapshotRecoverer {
            table_id: load_req.table_id,
            space_id: load_req.space_id,
            metaUpdateLogStore: log_store,
            metaUpdateSnapshotStore: snapshot_store,
        };

        let meta_snapshot_opt = snapshotRecoverer.recover().await?.and_then(|v| v.data);

        // Apply it to table.
        if let Some(snapshot) = meta_snapshot_opt {
            let meta_edit = MetaEdit::Snapshot(snapshot);
            let request = MetaEditRequest {
                tableShardInfo: TableShardInfo::new(load_req.shard_id),
                metaEdit: meta_edit,
            };
            self.tableMetaSet.apply_edit_to_table(request)?;
        }

        info!("Manifest recover finish, request:{load_req:?}");

        Ok(())
    }

    async fn do_snapshot(&self, request: SnapshotRequest) -> GenericResult<()> {
        info!("Manifest do snapshot, request:{:?}", request);

        let table_id = request.table_id;
        let location = WalLocation::new(request.shard_id as u64, table_id.as_u64());
        let space_id = request.space_id;
        let table_id = request.table_id;

        self.do_snapshot_internal(space_id, table_id, location).await.box_err()?;

        Ok(())
    }
}

#[async_trait]
trait MetaUpdateLogStore: fmt::Debug {
    type Iter: MetaUpdateLogEntryIterator + Send;

    async fn scan(&self, start: ReadBoundary) -> Result<Self::Iter>;
    async fn append(&self, meta_update: MetaUpdate) -> Result<SequenceNumber>;
    async fn delete_up_to(&self, inclusive_end: SequenceNumber) -> Result<()>;
}

#[async_trait]
trait MetaUpdateSnapshotStore: fmt::Debug {
    async fn store(&self, snapshot: &Snapshot) -> Result<()>;
    async fn load(&self) -> Result<Option<Snapshot>>;
}

#[derive(Debug)]
struct MetaUpdateSnapshotStoreObjectStoreBased {
    store: ObjectStoreRef,
    snapshot_path: Path,
}

impl MetaUpdateSnapshotStoreObjectStoreBased {
    const CURRENT_SNAPSHOT_NAME: &str = "current";
    const SNAPSHOT_PATH_PREFIX: &str = "manifest/snapshot";

    pub fn new(space_id: SpaceId, table_id: TableId, store: ObjectStoreRef) -> Self {
        let snapshot_path = Self::snapshot_path(space_id, table_id);
        Self {
            store,
            snapshot_path,
        }
    }

    fn snapshot_path(space_id: SpaceId, table_id: TableId) -> Path {
        format!(
            "{}/{}/{}/{}",
            Self::SNAPSHOT_PATH_PREFIX,
            space_id,
            table_id,
            Self::CURRENT_SNAPSHOT_NAME,
        )
            .into()
    }
}

#[async_trait]
impl MetaUpdateSnapshotStore for MetaUpdateSnapshotStoreObjectStoreBased {
    /// Store the latest snapshot to the underlying store by overwriting the old snapshot.
    async fn store(&self, snapshot: &Snapshot) -> Result<()> {
        let snapshot_pb = manifest_pb::Snapshot::from(snapshot.clone());
        let payload = snapshot_pb.encode_to_vec();
        // The atomic write is ensured by the [`ObjectStore`] implementation.
        self.store.put(&self.snapshot_path, payload.into()).await.context(StoreSnapshot)?;

        Ok(())
    }

    /// Load the `current_snapshot` file from the underlying store, and with the
    /// mapping info in it load the latest snapshot file then.
    async fn load(&self) -> Result<Option<Snapshot>> {
        let get_res = self.store.get(&self.snapshot_path).await;
        if let Err(object_store::ObjectStoreError::NotFound { path, source }) = &get_res {
            warn!(
                "Current snapshot file doesn't exist, path:{}, err:{}",
                path, source
            );
            return Ok(None);
        };

        // TODO: currently, this is just a workaround to handle the case where the error
        // is not thrown as [object_store::ObjectStoreError::NotFound].
        if let Err(err) = &get_res {
            let err_msg = err.to_string().to_lowercase();
            if err_msg.contains("404") || err_msg.contains("not found") {
                warn!("Current snapshot file doesn't exist, err:{}", err);
                return Ok(None);
            }
        }

        let payload = get_res
            .context(FetchSnapshot)?
            .bytes()
            .await
            .context(FetchSnapshot)?;
        let snapshot_pb =
            manifest_pb::Snapshot::decode(payload.as_bytes()).context(DecodeSnapshot)?;
        let snapshot = Snapshot::try_from(snapshot_pb)
            .box_err()
            .context(LoadSnapshot)?;

        Ok(Some(snapshot))
    }
}

#[derive(Debug, Clone)]
struct MetaUpdateLogStoreWalBased {
    opts: Options,
    location: WalLocation,
    walManager: Arc<dyn WalManager>,
}

#[async_trait]
impl MetaUpdateLogStore for MetaUpdateLogStoreWalBased {
    type Iter = MetaUpdateReaderImpl;

    async fn scan(&self, start: ReadBoundary) -> Result<Self::Iter> {
        let ctx = ReadContext {
            timeout: self.opts.scan_timeout.0,
            batch_size: self.opts.scan_batch_size,
        };

        let read_req = ReadRequest {
            location: self.location,
            start,
            end: ReadBoundary::Max,
        };

        let iterator = self.walManager.read_batch(&ctx, &read_req).await.context(ReadWal)?;

        Ok(MetaUpdateReaderImpl {
            iterator,
            has_next: true,
            buffer: VecDeque::with_capacity(ctx.batch_size),
        })
    }

    async fn append(&self, meta_update: MetaUpdate) -> Result<SequenceNumber> {
        let payload = MetaUpdatePayload::from(meta_update);
        let logBatchEncoder = LogBatchEncoder::create(self.location);

        // fenquen 表的生成等使用proto来序列化 对应的byte变为后续的kv中的value
        let logWriteBatch = logBatchEncoder.encode(&payload).context(EncodePayloads {
            wal_location: self.location,
        })?;

        let write_ctx = WriteContext { timeout: self.opts.store_timeout.0 };

        self.walManager.write(&write_ctx, &logWriteBatch).await.context(WriteWal)
    }

    async fn delete_up_to(&self, inclusive_end: SequenceNumber) -> Result<()> {
        self.walManager
            .mark_delete_entries_up_to(self.location, inclusive_end)
            .await
            .context(CleanWal)
    }
}