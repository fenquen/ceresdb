// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst file and storage info

use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashSet},
    fmt,
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use common_types::{
    time::{TimeRange, Timestamp},
    SequenceNumber,
};
use log::{error, info, warn};
use macros::define_result;
use metric_ext::Meter;
use object_store::{ObjectStore, ObjectStoreRef};
use runtime::{JoinHandle, Runtime};
use snafu::{ResultExt, Snafu};
use table_engine::table::TableId;
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    Mutex,
};

use crate::{space::SpaceId, sst::manager::FileId, table::sst_util, table_options::StorageFormat};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to join purger, err:{}", source))]
    StopPurger { source: runtime::Error },
}

define_result!(Error);

/// currently there are only two levels: 0, 1.
pub const SST_LEVEL_NUM: usize = 2;

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq)]
pub struct Level(u16);

impl Level {
    pub const MAX: Self = Self(1);
    pub const MIN: Self = Self(0);

    pub fn next(&self) -> Self {
        Self::MAX.0.min(self.0 + 1).into()
    }

    pub fn is_min(&self) -> bool {
        self == &Self::MIN
    }

    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }

    pub fn as_u32(&self) -> u32 {
        self.0 as u32
    }

    pub fn as_u16(&self) -> u16 {
        self.0
    }
}

impl From<u16> for Level {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// TODO(yingwen): Order or split file by time range to speed up filter (even in level 0).
/// Manage files of single level
pub struct LevelHandler {
    pub level: Level,
    /// All files in current level.
    pub fileHandleSet: FileHandleSet,
}

impl LevelHandler {
    pub fn new(level: Level) -> Self {
        Self {
            level,
            fileHandleSet: FileHandleSet::default(),
        }
    }

    #[inline]
    pub fn insert(&mut self, file: FileHandle) {
        self.fileHandleSet.insert(file);
    }

    pub fn latest_sst(&self) -> Option<FileHandle> {
        self.fileHandleSet.latest()
    }

    pub fn pickSsts(&self, timeRange: TimeRange) -> Vec<FileHandle> {
        self.fileHandleSet.getFileHandlesByTimeRange(timeRange)
    }

    #[inline]
    pub fn remove_ssts(&mut self, file_ids: &[FileId]) {
        self.fileHandleSet.remove_by_ids(file_ids);
    }

    pub fn iter_ssts(&self) -> Iter {
        let iter = self.fileHandleSet.fileOrderKey_fileHandle.values();
        Iter(iter)
    }

    #[inline]
    pub fn collect_expired(&self,
                           expire_time: Option<Timestamp>,
                           expired_files: &mut Vec<FileHandle>) {
        self.fileHandleSet.collect_expired(expire_time, expired_files);
    }

    #[inline]
    pub fn has_expired_sst(&self, expire_time: Option<Timestamp>) -> bool {
        self.fileHandleSet.has_expired_sst(expire_time)
    }
}

pub struct Iter<'a>(std::collections::btree_map::Values<'a, FileOrdKey, FileHandle>);

impl<'a> Iterator for Iter<'a> {
    type Item = &'a FileHandle;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

#[derive(Clone)]
pub struct FileHandle {
    pub fileHandleInner: Arc<FileHandleInner>,
}

impl PartialEq for FileHandle {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl Eq for FileHandle {}

impl Hash for FileHandle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state);
    }
}

impl FileHandle {
    pub fn new(meta: FileMeta, purge_queue: FilePurgeQueue) -> Self {
        Self {
            fileHandleInner: Arc::new(FileHandleInner {
                meta,
                purge_queue,
                compacting: AtomicBool::new(false),
                metrics: SstMetrics::default(),
            }),
        }
    }

    #[inline]
    pub fn read_meter(&self) -> Arc<Meter> {
        self.fileHandleInner.metrics.read_meter.clone()
    }

    #[inline]
    pub fn row_num(&self) -> u64 {
        self.fileHandleInner.meta.row_num
    }

    #[inline]
    pub fn id(&self) -> FileId {
        self.fileHandleInner.meta.id
    }

    #[inline]
    pub fn id_ref(&self) -> &FileId {
        &self.fileHandleInner.meta.id
    }

    #[inline]
    pub fn intersectWith(&self, timeRange: TimeRange) -> bool {
        self.fileHandleInner.meta.timeRange.intersectWith(timeRange)
    }

    #[inline]
    pub fn time_range(&self) -> TimeRange {
        self.fileHandleInner.meta.timeRange
    }

    #[inline]
    pub fn time_range_ref(&self) -> &TimeRange {
        &self.fileHandleInner.meta.timeRange
    }

    #[inline]
    pub fn max_sequence(&self) -> SequenceNumber {
        self.fileHandleInner.meta.max_seq
    }

    #[inline]
    pub fn being_compacted(&self) -> bool {
        self.fileHandleInner.compacting.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn size(&self) -> u64 {
        self.fileHandleInner.meta.size
    }

    #[inline]
    pub fn set_being_compacted(&self, value: bool) {
        self.fileHandleInner.compacting.store(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn storage_format(&self) -> StorageFormat {
        self.fileHandleInner.meta.storage_format
    }

    #[inline]
    pub fn meta(&self) -> FileMeta {
        self.fileHandleInner.meta.clone()
    }
}

impl fmt::Debug for FileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileHandle")
            .field("meta", &self.fileHandleInner.meta)
            .field("being_compacted", &self.being_compacted())
            .finish()
    }
}

struct SstMetrics {
    pub read_meter: Arc<Meter>,
    pub key_num: usize,
}

impl Default for SstMetrics {
    fn default() -> Self {
        SstMetrics {
            read_meter: Arc::new(Meter::new()),
            key_num: 0,
        }
    }
}

impl Debug for SstMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SstMetrics")
            .field("read_meter", &self.read_meter.h2_rate())
            .field("key_num", &self.key_num)
            .finish()
    }
}

pub struct FileHandleInner {
    pub meta: FileMeta,
    purge_queue: FilePurgeQueue,
    compacting: AtomicBool,
    metrics: SstMetrics,
}

impl Drop for FileHandleInner {
    fn drop(&mut self) {
        info!("fileHandle is dropped, meta:{:?}", self.meta);

        // Push file cannot block or be async because we are in drop().
        self.purge_queue.push_file(self.meta.id);
    }
}

/// Used to order [FileHandle] by (end_time, start_time, file_id) 注意字段的顺序不能动
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct FileOrdKey {
    exclusive_end: Timestamp,
    inclusive_start: Timestamp,
    file_id: FileId,
}

impl FileOrdKey {
    fn for_seek(exclusive_end: Timestamp) -> Self {
        Self {
            exclusive_end,
            inclusive_start: Timestamp::MIN,
            file_id: 0,
        }
    }

    fn buildByFileHandle(file: &FileHandle) -> Self {
        Self {
            exclusive_end: file.time_range().exclusive_end,
            inclusive_start: file.time_range().inclusive_start,
            file_id: file.id(),
        }
    }
}

/// Used to index [FileHandle] by file_id
struct FileHandleHash(FileHandle);

impl PartialEq for FileHandleHash {
    fn eq(&self, other: &Self) -> bool {
        self.0.id() == other.0.id()
    }
}

impl Eq for FileHandleHash {}

impl Hash for FileHandleHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.id().hash(state);
    }
}

impl Borrow<FileId> for FileHandleHash {
    #[inline]
    fn borrow(&self) -> &FileId {
        self.0.id_ref()
    }
}

#[derive(Default)]
pub struct FileHandleSet {
    /// files ordered by time range and id.
    fileOrderKey_fileHandle: BTreeMap<FileOrdKey, FileHandle>,
    /// files indexed by file id, used to speed up removal.
    id_to_files: HashSet<FileHandleHash>,
}

impl FileHandleSet {
    fn latest(&self) -> Option<FileHandle> {
        if let Some(file) = self.fileOrderKey_fileHandle.values().rev().next() {
            return Some(file.clone());
        }
        None
    }

    pub fn getFileHandlesByTimeRange(&self, timeRange: TimeRange) -> Vec<FileHandle> {
        // 和之前搜索memTable时候的套路相同 seek to first sst whose end time >= time_range.inclusive_start().
        let seek_key = FileOrdKey::for_seek(timeRange.inclusive_start);
        self.fileOrderKey_fileHandle.range(seek_key..).filter_map(|(_key, fileHandle)| {
            if fileHandle.intersectWith(timeRange) {
                Some(fileHandle.clone())
            } else {
                None
            }
        }).collect()
    }

    fn insert(&mut self, file: FileHandle) {
        self.fileOrderKey_fileHandle.insert(FileOrdKey::buildByFileHandle(&file), file.clone());
        self.id_to_files.insert(FileHandleHash(file));
    }

    fn remove_by_ids(&mut self, file_ids: &[FileId]) {
        for file_id in file_ids {
            if let Some(file) = self.id_to_files.take(file_id) {
                let key = FileOrdKey::buildByFileHandle(&file.0);
                self.fileOrderKey_fileHandle.remove(&key);
            }
        }
    }

    /// Collect ssts with time range is expired.
    fn collect_expired(&self, expire_time: Option<Timestamp>, expired_files: &mut Vec<FileHandle>) {
        for file in self.fileOrderKey_fileHandle.values() {
            if file.time_range().is_expired(expire_time) {
                expired_files.push(file.clone());
            } else {
                // Files are sorted by end time first, so there is no more file whose end time is less than `expire_time`.
                break;
            }
        }
    }

    fn has_expired_sst(&self, expire_time: Option<Timestamp>) -> bool {
        // Files are sorted by end time first, so check first file is enough.
        if let Some(file) = self.fileOrderKey_fileHandle.values().next() {
            return file.time_range().is_expired(expire_time);
        }

        false
    }
}

/// Meta of a sst file, immutable once created
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    /// Id of the sst file
    pub id: FileId,
    /// File size in bytes
    pub size: u64,
    /// Total row number
    pub row_num: u64,
    /// The time range of the file.
    pub timeRange: TimeRange,
    /// The max sequence number of the file.
    pub max_seq: u64,
    /// The format of the file.
    pub storage_format: StorageFormat,
}

// Queue to store files to be deleted for a table.
#[derive(Clone)]
pub struct FilePurgeQueue {
    // Wrap a inner struct to avoid storing space/table ids for each file.
    inner: Arc<FilePurgeQueueInner>,
}

impl FilePurgeQueue {
    pub fn new(space_id: SpaceId, table_id: TableId, sender: UnboundedSender<Request>) -> Self {
        Self {
            inner: Arc::new(FilePurgeQueueInner {
                space_id,
                table_id,
                sender,
                closed: AtomicBool::new(false),
            }),
        }
    }

    /// Close the purge queue, then all request pushed to this queue will be
    /// ignored. This is mainly used to avoid files being deleted after the
    /// db is closed.
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::SeqCst);
    }

    fn push_file(&self, file_id: FileId) {
        if self.inner.closed.load(Ordering::SeqCst) {
            warn!("purger closed, ignore file_id:{file_id}");
            return;
        }

        // Send the file id via a channel to file purger and delete the file from sst store in background.
        let request = FilePurgeRequest {
            space_id: self.inner.space_id,
            table_id: self.inner.table_id,
            file_id,
        };

        if let Err(send_res) = self.inner.sender.send(Request::Purge(request)) {
            error!("failed to send delete file request, request:{:?}",send_res.0);
        }
    }
}

struct FilePurgeQueueInner {
    space_id: SpaceId,
    table_id: TableId,
    closed: AtomicBool,
    sender: UnboundedSender<Request>,
}

#[derive(Debug)]
pub struct FilePurgeRequest {
    space_id: SpaceId,
    table_id: TableId,
    file_id: FileId,
}

#[derive(Debug)]
pub enum Request {
    Purge(FilePurgeRequest),
    Exit,
}

/// Background file purger.
pub struct FilePurger {
    sender: UnboundedSender<Request>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl FilePurger {
    pub fn start(runtime: &Runtime, store: ObjectStoreRef) -> Self {
        // we must use unbound channel, so the sender wont block when the handle is dropped.
        let (sender, receiver) = mpsc::unbounded_channel();

        // Spawn a background job to purge files.
        let handle = runtime.spawn(async {
            Self::purge_file_loop(store, receiver).await;
        });

        Self {
            sender,
            handle: Mutex::new(Some(handle)),
        }
    }

    pub async fn stop(&self) -> Result<()> {
        info!("Try to stop file purger");

        if self.sender.send(Request::Exit).is_err() {
            error!("File purge task already exited");
        }

        let mut handle = self.handle.lock().await;
        // Also clear the handle to avoid await a ready future.
        if let Some(h) = handle.take() {
            h.await.context(StopPurger)?;
        }

        Ok(())
    }

    pub fn create_purge_queue(&self, space_id: SpaceId, table_id: TableId) -> FilePurgeQueue {
        FilePurgeQueue::new(space_id, table_id, self.sender.clone())
    }

    async fn purge_file_loop(store: Arc<dyn ObjectStore>, mut receiver: UnboundedReceiver<Request>) {
        info!("file purger start");

        while let Some(request) = receiver.recv().await {
            match request {
                Request::Purge(purge_request) => {
                    let sst_file_path = sst_util::new_sst_file_path(
                        purge_request.space_id,
                        purge_request.table_id,
                        purge_request.file_id,
                    );

                    info!("file purger delete file, purge_request:{:?}, sst_file_path:{}",purge_request,sst_file_path.to_string());

                    if let Err(e) = store.delete(&sst_file_path).await {
                        error!("file purger failed to delete file, sst_file_path:{}, err:{}",sst_file_path.to_string(),e);
                    }
                }
                Request::Exit => break,
            }
        }

        info!("file purger exit");
    }
}