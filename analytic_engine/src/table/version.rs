// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table version

use std::{
    cmp,
    collections::{BTreeMap, HashMap},
    fmt,
    ops::Bound,
    sync::{Arc, RwLock},
    time::Duration,
};

use common_types::{
    row::Row,
    schema::{self, Schema},
    time::{TimeRange, Timestamp},
    SequenceNumber,
};
use macros::define_result;
use snafu::{ensure, Backtrace, ResultExt, Snafu};

use crate::{
    compaction::{
        picker::{self, CompactionPickerRef, PickerContext},
        CompactionTask, ExpiredFiles,
    },
    memtable::{self, key::KeySequence, MemTableRef, PutContext},
    sampler::{DefaultSampler, SamplerRef},
    sst::{
        file::{FileHandle, FilePurgeQueue, SST_LEVEL_NUM},
        manager::{FileId, LevelsController},
    },
    table::{
        data::{MemTableId, DEFAULT_ALLOC_STEP},
        version_edit::{AddFile, VersionEdit},
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
    "Schema mismatch, memtable_version:{}, given:{}.\nBacktrace:\n{}",
    memtable_version,
    given,
    backtrace
    ))]
    SchemaMismatch {
        memtable_version: schema::Version,
        given: schema::Version,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to put memtable, err:{}", source))]
    PutMemTable { source: crate::memtable::Error },

    #[snafu(display("Failed to collect timestamp, err:{}", source))]
    CollectTimestamp { source: crate::sampler::Error },
}

define_result!(Error);

/// Memtable for sampling timestamp.
#[derive(Clone)]
pub struct SamplingMemTable {
    pub mem: MemTableRef,
    pub id: MemTableId,
    /// If freezed is true, the sampling is finished and no more data should be
    /// inserted into this memtable. Otherwise, the memtable is active and all
    /// data should ONLY write to this memtable instead of mutable memtable.
    pub freezed: bool,
    pub sampler: SamplerRef,
}

impl SamplingMemTable {
    pub fn new(memTable: MemTableRef, id: MemTableId) -> Self {
        SamplingMemTable {
            mem: memTable,
            id,
            freezed: false,
            sampler: Arc::new(DefaultSampler::default()),
        }
    }

    pub fn last_sequence(&self) -> SequenceNumber {
        self.mem.last_sequence()
    }

    fn memory_usage(&self) -> usize {
        self.mem.approximate_memory_usage()
    }

    /// Suggest segment duration, if there is no sampled timestamp, returns
    /// default segment duration.
    fn suggest_segment_duration(&self) -> Duration {
        self.sampler.suggest_duration()
    }
}

impl fmt::Debug for SamplingMemTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SamplingMemTable")
            .field("id", &self.id)
            .field("freezed", &self.freezed)
            .finish()
    }
}

/// Memtable with additional meta data
#[derive(Clone)]
pub struct MemTableState {
    pub memTable: MemTableRef,
    /// The `time_range` is estimated via the time range of the first row group write to  memtable and is aligned to segment size
    pub timeRange: TimeRange,
    /// newer memtable has greater id
    pub id: MemTableId,
}

impl MemTableState {
    #[inline]
    pub fn lastSequence(&self) -> SequenceNumber {
        self.memTable.last_sequence()
    }
}

impl fmt::Debug for MemTableState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemTableState")
            .field("time_range", &self.timeRange)
            .field("id", &self.id)
            .field("mem", &self.memTable.approximate_memory_usage())
            .field("metrics", &self.memTable.metrics())
            .field("last_sequence", &self.memTable.last_sequence())
            .finish()
    }
}

// TODO(yingwen): Replace by Either.
#[derive(Clone)]
pub enum MemTableForWrite {
    Sampling(SamplingMemTable),
    Normal(MemTableState),
}

impl MemTableForWrite {
    #[inline]
    pub fn setLastSequence(&self, sequenceNumber: SequenceNumber) -> memtable::Result<()> {
        self.memtable().set_last_sequence(sequenceNumber)
    }

    #[inline]
    pub fn acceptTimestamp(&self, timestamp: Timestamp) -> bool {
        match self {
            MemTableForWrite::Sampling(_) => true,
            MemTableForWrite::Normal(v) => v.timeRange.contains(timestamp),
        }
    }

    #[inline]
    pub fn put(&self,
               ctx: &mut PutContext,
               keySequence: KeySequence,
               row: &Row,
               schema: &Schema,
               timestamp: Timestamp) -> Result<()> {
        match self {
            MemTableForWrite::Sampling(v) => {
                v.mem.put(ctx, keySequence, row, schema).context(PutMemTable)?;

                // Collect the timestamp of this row.
                v.sampler.collect(timestamp).context(CollectTimestamp)?;

                Ok(())
            }
            MemTableForWrite::Normal(memTableState) => {
                memTableState.memTable.put(ctx, keySequence, row, schema).context(PutMemTable)
            }
        }
    }

    #[inline]
    fn memtable(&self) -> &MemTableRef {
        match self {
            MemTableForWrite::Sampling(v) => &v.mem,
            MemTableForWrite::Normal(v) => &v.memTable,
        }
    }

    pub fn as_sampling(&self) -> &SamplingMemTable {
        match self {
            MemTableForWrite::Sampling(v) => v,
            MemTableForWrite::Normal(_) => panic!(),
        }
    }

    pub fn as_normal(&self) -> &MemTableState {
        match self {
            MemTableForWrite::Sampling(_) => panic!(),
            MemTableForWrite::Normal(v) => v,
        }
    }
}

#[derive(Debug, Default)]
pub struct FlushableMemTables {
    pub sampling_mem: Option<SamplingMemTable>,
    pub memtables: Vec<MemTableState>,
}

impl FlushableMemTables {
    pub fn ids(&self) -> Vec<MemTableId> {
        let mut memtable_ids = Vec::with_capacity(self.memtables.len() + 1);

        if let Some(v) = &self.sampling_mem {
            memtable_ids.push(v.id);
        }
        for mem in &self.memtables {
            memtable_ids.push(mem.id);
        }

        memtable_ids
    }

    pub fn len(&self) -> usize {
        self.sampling_mem.as_ref().map_or(0, |_| 1) + self.memtables.len()
    }
}

pub type MemTableVec = Vec<MemTableState>;

/// MemTableView holds all memtables of the table
#[derive(Debug)]
struct MemTableView {
    /// The memtable for sampling timestamp to suggest segment duration.
    ///
    /// This memtable is special and may contains data in differnt segment, so
    /// can not be moved into immutable memtable set.
    samplingMemTable: Option<SamplingMemTable>,

    /// mutable memTables 使用endTime定位 fenquen
    mutableMemTableSet: MutableMemTableSet,

    /// immutable memTables 使用 memTableId定位 fenquen
    immutableMemTableSet: ImmutableMemTableSet,
}

impl MemTableView {
    fn new() -> Self {
        Self {
            samplingMemTable: None,
            mutableMemTableSet: MutableMemTableSet::new(),
            immutableMemTableSet: ImmutableMemTableSet(BTreeMap::new()),
        }
    }

    fn mutable_memory_usage(&self) -> usize {
        self.mutableMemTableSet.memory_usage()
            + self.samplingMemTable.as_ref().map(|v| v.memory_usage()).unwrap_or(0)
    }

    /// get the total memory usage of mutable and immutable memtables.
    fn total_memory_usage(&self) -> usize {
        let mutable_usage = self.mutable_memory_usage();
        let immutable_usage = self.immutableMemTableSet.memory_usage();

        mutable_usage + immutable_usage
    }

    /// Instead of replace the old memtable by a new memtable, we just move the
    /// old memtable to immutable memtables and left mutable memtables empty.
    /// New mutable memtable will be constructed via put request.
    fn switchMutableMemTable2Immutable(&mut self) -> Option<SequenceNumber> {
        self.mutableMemTableSet.move_to_inmem(&mut self.immutableMemTableSet)
    }

    /// Sample the segment duration.
    ///
    /// If the sampling memtable is still active, return the suggested segment
    /// duration or move all mutable memtables into immutable memtables if
    /// the sampling memtable is freezed and returns None.
    fn suggest_duration(&mut self) -> Option<Duration> {
        if let Some(v) = &mut self.samplingMemTable {
            if !v.freezed {
                // other memtable should be empty during sampling phase.
                assert!(self.mutableMemTableSet.is_empty());
                assert!(self.immutableMemTableSet.is_empty());

                // the sampling memtable is still active, we need to compute the segment duration and then freeze the memtable.
                let segment_duration = v.suggest_segment_duration();

                // but we cannot freeze the sampling memtable now, because the segment duration may not yet been persisted.
                return Some(segment_duration);
            }
        }

        None
    }

    fn freeze_sampling_memtable(&mut self) -> Option<SequenceNumber> {
        if let Some(v) = &mut self.samplingMemTable {
            v.freezed = true;
            return Some(v.mem.last_sequence());
        }
        None
    }

    /// Returns memtables need to be flushed. Only sampling memtable and
    /// immutables will be considered. And only memtables which `last_sequence` less or equal to the given [SequenceNumber] will be picked.
    ///
    /// This method assumes that one sequence number will not exist in multiple memtables.
    fn pick_memtables_to_flush(&self, lastSequence: SequenceNumber) -> FlushableMemTables {
        let mut mems = FlushableMemTables::default();

        if let Some(v) = &self.samplingMemTable {
            if v.last_sequence() <= lastSequence {
                mems.sampling_mem = Some(v.clone());
            }
        }

        for memTableState in self.immutableMemTableSet.0.values() {
            if memTableState.lastSequence() <= lastSequence {
                mems.memtables.push(memTableState.clone());
            }
        }

        mems
    }

    /// Remove memtable from immutables or sampling memtable.
    #[inline]
    fn remove_immutable_or_sampling(&mut self, id: MemTableId) {
        if let Some(v) = &self.samplingMemTable {
            if v.id == id {
                self.samplingMemTable = None;
                return;
            }
        }

        self.immutableMemTableSet.0.remove(&id);
    }

    /// 得到和指定的timeRange有交集的memtable
    fn memTablesForRead(&self,
                        timeRange: TimeRange,
                        memTableStateVec: &mut Vec<MemTableState>,
                        sampling_mem: &mut Option<SamplingMemTable>) {
        self.mutableMemTableSet.memTablesForRead(timeRange, memTableStateVec);
        self.immutableMemTableSet.memTablesForRead(timeRange, memTableStateVec);

        *sampling_mem = self.samplingMemTable.clone();
    }
}

/// Mutable memtables
///
/// All mutable memtables ordered by their end time (exclusive), their time
/// range may overlaps if `alter segment duration` is supported
///
/// We choose end time so we can use BTreeMap::range to find the first range
/// that may contains a given timestamp (end >= timestamp)
#[derive(Debug)]
struct MutableMemTableSet(BTreeMap<Timestamp, MemTableState>);

impl MutableMemTableSet {
    fn new() -> MutableMemTableSet {
        MutableMemTableSet(BTreeMap::new())
    }

    /// Get memtale by timestamp for write
    fn memtable_for_write(&self, timestamp: Timestamp) -> Option<&MemTableState> {
        // Find the first memtable whose end time (exclusive) > timestamp
        if let Some((_, memtable)) = self.0.range((Bound::Excluded(timestamp), Bound::Unbounded)).next() {
            if memtable.timeRange.contains(timestamp) {
                return Some(memtable);
            }
        }

        None
    }

    /// insert memtable, the caller should guarantee the key of memtable is not present
    fn insert(&mut self, memTableState: MemTableState) -> Option<MemTableState> {
        // Use end time of time range as key
        let end = memTableState.timeRange.exclusive_end();
        self.0.insert(end, memTableState)
    }

    fn memory_usage(&self) -> usize {
        self.0.values().map(|m| m.memTable.approximate_memory_usage()).sum()
    }

    /// move all mutable memtables to immutable memtables.
    fn move_to_inmem(&mut self, immutableMemTableSet: &mut ImmutableMemTableSet) -> Option<SequenceNumber> {
        let lastSeq = self.0.values().map(|memTableState| {
            let lastSeq = memTableState.memTable.last_sequence();
            immutableMemTableSet.0.insert(memTableState.id, memTableState.clone());

            lastSeq
        }).max();

        self.0.clear();
        lastSeq
    }

    fn memTablesForRead(&self, timeRange: TimeRange, memTableStateVec: &mut Vec<MemTableState>) {
        // seek to first memtable whose end time (exclusive) > time_range.start
        for (_endTs, memTableState) in self.0.range((Bound::Excluded(timeRange.inclusive_start), Bound::Unbounded)) {
            // we need to iterate all candidate memtables as their start time is unspecific
            if memTableState.timeRange.intersectWith(timeRange) {
                memTableStateVec.push(memTableState.clone());
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// MemTables are ordered by memtable id, so lookup by memtable id is fast
#[derive(Debug)]
struct ImmutableMemTableSet(BTreeMap<MemTableId, MemTableState>);

impl ImmutableMemTableSet {
    /// Memory used by all immutable memtables
    fn memory_usage(&self) -> usize {
        self.0.values().map(|m| m.memTable.approximate_memory_usage()).sum()
    }

    fn memTablesForRead(&self, timeRange: TimeRange, memTableStateVec: &mut MemTableVec) {
        for mem in self.0.values() {
            if mem.timeRange.intersectWith(timeRange) {
                memTableStateVec.push(mem.clone());
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

pub type LeveledFiles = Vec<Vec<FileHandle>>;

/// Memtable/sst to read for given time range.
pub struct ReadView {
    pub sampling_mem: Option<SamplingMemTable>,
    pub memTableStateVec: Vec<MemTableState>,
    /// ssts to read in each level,MUST ensure the length of `leveled_ssts` >= MAX_LEVEL.
    pub allLeveSatisfiedSsts: Vec<Vec<FileHandle>>,
}

impl Default for ReadView {
    fn default() -> Self {
        Self {
            sampling_mem: None,
            memTableStateVec: Vec::new(),
            allLeveSatisfiedSsts: vec![Vec::new(); SST_LEVEL_NUM],
        }
    }
}

impl ReadView {
    pub fn contains_sampling(&self) -> bool {
        self.sampling_mem.is_some()
    }
}

/// Data of TableVersion
struct TableVersionInner {
    /// All memtables
    memTableView: MemTableView,

    /// All ssts
    levelsController: LevelsController,

    /// The earliest sequence number of the entries already flushed (inclusive).
    /// All log entry with sequence <= `flushed_sequence` can be deleted
    flushed_sequence: SequenceNumber,

    /// Max id of the sst file.
    ///
    /// The id is allocated by step, so there are some still unused ids smaller
    /// than the max one. And this field is only a mem state for Manifest,
    /// it can only be updated during recover or by Manifest.
    max_file_id: FileId,
}

impl TableVersionInner {
    fn memtable_for_write(&self, timestamp: Timestamp) -> Option<MemTableForWrite> {
        if let Some(mem) = self.memTableView.samplingMemTable.clone() {
            if !mem.freezed {
                // If sampling memtable is not freezed.
                return Some(MemTableForWrite::Sampling(mem));
            }
        }

        self.memTableView
            .mutableMemTableSet
            .memtable_for_write(timestamp)
            .cloned()
            .map(MemTableForWrite::Normal)
    }
}

// TODO(yingwen): How to support snapshot?
/// Table version
///
/// Holds memtables and sst meta data of a table
///
/// Switching memtable, memtable to level 0 file, addition/deletion to files should be done atomically.
pub struct TableVersion {
    tableVersionInner: RwLock<TableVersionInner>,
}

impl TableVersion {
    /// Create an empty table version
    pub fn new(purge_queue: FilePurgeQueue) -> Self {
        Self {
            tableVersionInner: RwLock::new(TableVersionInner {
                memTableView: MemTableView::new(),
                levelsController: LevelsController::new(purge_queue),
                flushed_sequence: 0,
                max_file_id: 0,
            }),
        }
    }

    /// See [MemTableView::mutable_memory_usage]
    pub fn mutable_memory_usage(&self) -> usize {
        self.tableVersionInner.read().unwrap().memTableView.mutable_memory_usage()
    }

    /// See [MemTableView::total_memory_usage]
    pub fn total_memory_usage(&self) -> usize {
        self.tableVersionInner.read().unwrap().memTableView.total_memory_usage()
    }

    /// return the suggested segment duration if sampling memtable is still active
    pub fn suggest_duration(&self) -> Option<Duration> {
        self.tableVersionInner.write().unwrap().memTableView.suggest_duration()
    }

    /// Switch all mutable memtables
    ///
    /// Returns the maxium `SequenceNumber` in the mutable memtables needs to be freezed.
    pub fn switchMutableMemTable2Immutable(&self) -> Option<SequenceNumber> {
        self.tableVersionInner.write().unwrap().memTableView.switchMutableMemTable2Immutable()
    }

    /// Stop timestamp sampling and freezed the sampling memtable.
    ///
    /// REQUIRE: Do in write worker
    pub fn freeze_sampling_memtable(&self) -> Option<SequenceNumber> {
        self.tableVersionInner.write().unwrap().memTableView.freeze_sampling_memtable()
    }

    /// See [MemTableView::pick_memtables_to_flush]
    pub fn pick_memtables_to_flush(&self, lastSequence: SequenceNumber) -> FlushableMemTables {
        self.tableVersionInner.read().unwrap().memTableView.pick_memtables_to_flush(lastSequence)
    }

    /// Get memtable by timestamp for write.
    ///
    /// The returned schema is guaranteed to have schema with same version as
    /// `schema_version`. Return None if the schema of existing memtable has different schema.
    pub fn memtable_for_write(&self,
                              timestamp: Timestamp,
                              schema_version: schema::Version) -> Result<Option<MemTableForWrite>> {
        // Find memtable by timestamp
        let mutable = match self.tableVersionInner.read().unwrap().memtable_for_write(timestamp) {
            Some(v) => v,
            None => return Ok(None),
        };

        // We consider the schemas are same if they have the same version.
        ensure!(
            mutable.memtable().schema().version() == schema_version,
            SchemaMismatch {
                memtable_version: mutable.memtable().schema().version(),
                given: schema_version,
            }
        );

        Ok(Some(mutable))
    }

    /// Insert memtable into mutable memtable set.
    pub fn insertMutableMemTable(&self, memTableState: MemTableState) {
        let mut inner = self.tableVersionInner.write().unwrap();
        let old = inner.memTableView.mutableMemTableSet.insert(memTableState.clone());
        assert!(old.is_none(), "find a duplicate memtable, new_memtable:{:?}, old_memtable:{:?}, memtable_view:{:#?}", memTableState, old, inner.memTableView);
    }

    /// Set sampling memtable.
    ///
    /// Panic if the sampling memtable of this version is not None.
    pub fn set_sampling(&self, sampling_mem: SamplingMemTable) {
        let mut inner = self.tableVersionInner.write().unwrap();
        assert!(inner.memTableView.samplingMemTable.is_none());
        inner.memTableView.samplingMemTable = Some(sampling_mem);
    }

    /// Atomically apply the edit to the version.
    pub fn apply_edit(&self, edit: VersionEdit) {
        let mut inner = self.tableVersionInner.write().unwrap();

        // TODO(yingwen): else, log warning
        inner.flushed_sequence = cmp::max(inner.flushed_sequence, edit.flushed_sequence);

        inner.max_file_id = cmp::max(inner.max_file_id, edit.max_file_id);

        // Add sst files to level first.
        for add_file in edit.files_to_add {
            inner
                .levelsController
                .add_sst_to_level(add_file.level, add_file.file);
        }

        // Remove ssts from level.
        for delete_file in edit.files_to_delete {
            inner
                .levelsController
                .remove_ssts_from_level(delete_file.level, &[delete_file.file_id]);
        }

        // Remove immutable memtables.
        for mem_id in edit.mems_to_remove {
            inner.memTableView.remove_immutable_or_sampling(mem_id);
        }
    }

    /// Atomically apply the meta to the version, useful in recover.
    pub fn apply_meta(&self, meta: TableVersionMeta) {
        let mut inner = self.tableVersionInner.write().unwrap();

        inner.flushed_sequence = cmp::max(inner.flushed_sequence, meta.flushed_sequence);

        inner.max_file_id = cmp::max(inner.max_file_id, meta.max_file_id);

        for add_file in meta.files.into_values() {
            inner.levelsController.add_sst_to_level(add_file.level, add_file.file);
        }
    }

    /// 得到对应的timeRange的memTable和sst
    pub fn pickReadView(&self, timeRange: TimeRange) -> ReadView {
        let mut sampling_mem = None;
        let mut memTableStateVec = Vec::new();

        let mut allLeveSatisfiedSsts = vec![Vec::new(); SST_LEVEL_NUM];

        {
            // 得到 memtables for read.
            let tableVersionInner = self.tableVersionInner.read().unwrap();
            tableVersionInner.memTableView.memTablesForRead(timeRange, &mut memTableStateVec, &mut sampling_mem);

            // 得到 ssts for read.
            tableVersionInner.levelsController.pickSsts(timeRange, |level, satisfiedSstsInOneLevel| {
                allLeveSatisfiedSsts[level.as_usize()].extend_from_slice(satisfiedSstsInOneLevel)
            });
        }

        ReadView {
            sampling_mem,
            memTableStateVec,
            allLeveSatisfiedSsts,
        }
    }

    /// Pick ssts for compaction using given `picker`.
    pub fn pick_for_compaction(&self,
                               picker_ctx: PickerContext,
                               picker: &CompactionPickerRef) -> picker::Result<CompactionTask> {
        let mut inner = self.tableVersionInner.write().unwrap();

        picker.pick_compaction(picker_ctx, &mut inner.levelsController)
    }

    pub fn has_expired_sst(&self, expire_time: Option<Timestamp>) -> bool {
        let inner = self.tableVersionInner.read().unwrap();

        inner.levelsController.has_expired_sst(expire_time)
    }

    pub fn expired_ssts(&self, expire_time: Option<Timestamp>) -> Vec<ExpiredFiles> {
        let inner = self.tableVersionInner.read().unwrap();

        inner.levelsController.expired_ssts(expire_time)
    }

    pub fn flushed_sequence(&self) -> SequenceNumber {
        let inner = self.tableVersionInner.read().unwrap();

        inner.flushed_sequence
    }

    pub fn snapshot(&self) -> TableVersionSnapshot {
        let inner = self.tableVersionInner.read().unwrap();
        let controller = &inner.levelsController;
        let files = controller
            .levels()
            .flat_map(|level| {
                let ssts = controller.iter_ssts_at_level(level);
                ssts.map(move |file| {
                    let add_file = AddFile {
                        level,
                        file: file.meta(),
                    };
                    (file.id(), add_file)
                })
            })
            .collect();

        TableVersionSnapshot {
            flushed_sequence: inner.flushed_sequence,
            files,
            max_file_id: inner.max_file_id,
        }
    }
}

pub struct TableVersionSnapshot {
    pub flushed_sequence: SequenceNumber,
    pub files: HashMap<FileId, AddFile>,
    pub max_file_id: FileId,
}

/// During recovery, we apply all version edit to [TableVersionMeta] first, then
/// apply the version meta to the table, so we can avoid adding removed ssts to
/// the version.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableVersionMeta {
    pub flushed_sequence: SequenceNumber,
    pub files: HashMap<FileId, AddFile>,
    pub max_file_id: FileId,
}

impl TableVersionMeta {
    pub fn apply_edit(&mut self, edit: VersionEdit) {
        self.flushed_sequence = cmp::max(self.flushed_sequence, edit.flushed_sequence);

        for add_file in edit.files_to_add {
            self.max_file_id = cmp::max(self.max_file_id, add_file.file.id);

            self.files.insert(add_file.file.id, add_file);
        }

        self.max_file_id = cmp::max(self.max_file_id, edit.max_file_id);

        // aligned max file id.
        self.max_file_id =
            (self.max_file_id + DEFAULT_ALLOC_STEP - 1) / DEFAULT_ALLOC_STEP * DEFAULT_ALLOC_STEP;

        for delete_file in edit.files_to_delete {
            self.files.remove(&delete_file.file_id);
        }
    }

    /// Returns the max file id in the files to add.
    pub fn max_file_id_to_add(&self) -> FileId {
        self.max_file_id
    }

    pub fn ordered_files(&self) -> Vec<AddFile> {
        let mut files_vec: Vec<_> = self.files.values().cloned().collect();
        files_vec.sort_unstable_by_key(|file| file.file.id);

        files_vec
    }
}

