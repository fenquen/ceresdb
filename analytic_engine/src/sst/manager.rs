// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Multi-level SST management

use common_types::time::{TimeRange, Timestamp};

use crate::{
    compaction::ExpiredFiles,
    sst::file::{FileHandle, FileMeta, FilePurgeQueue, Iter, Level, LevelHandler},
};

/// Id for a sst file
pub type FileId = u64;

/// A table level manager that manages all the sst files of the table
pub struct LevelsController {
    levelHandlers: Vec<LevelHandler>,
    purge_queue: FilePurgeQueue,
}

impl Drop for LevelsController {
    fn drop(&mut self) {
        // Close the purge queue to avoid files being deleted.
        self.purge_queue.close();
    }
}

impl LevelsController {
    /// Create an empty LevelsController
    pub fn new(purge_queue: FilePurgeQueue) -> Self {
        Self {
            levelHandlers: (Level::MIN.as_u16()..=Level::MAX.as_u16())
                .map(|v| LevelHandler::new(v.into()))
                .collect::<Vec<_>>(),
            purge_queue,
        }
    }

    /// Add sst file to level
    ///
    /// Panic: If the level is greater than the max level
    pub fn add_sst_to_level(&mut self, level: Level, file_meta: FileMeta) {
        let level_handler = &mut self.levelHandlers[level.as_usize()];
        let file = FileHandle::new(file_meta, self.purge_queue.clone());

        level_handler.insert(file);
    }

    pub fn latest_sst(&self, level: Level) -> Option<FileHandle> {
        self.levelHandlers[level.as_usize()].latest_sst()
    }

    /// Pick the ssts and collect it by `append_sst`.
    pub fn pick_ssts(&self,
                     time_range: TimeRange,
                     mut append_sst: impl FnMut(Level, &[FileHandle])) {
        for levelHandler in self.levelHandlers.iter() {
            let ssts = levelHandler.pick_ssts(time_range);
            append_sst(levelHandler.level, &ssts);
        }
    }

    /// Remove sst files from level.
    ///
    /// Panic: If the level is greater than the max level
    pub fn remove_ssts_from_level(&mut self, level: Level, file_ids: &[FileId]) {
        let level_handler = &mut self.levelHandlers[level.as_usize()];
        level_handler.remove_ssts(file_ids);
    }

    pub fn levels(&self) -> impl Iterator<Item=Level> + '_ {
        self.levelHandlers.iter().map(|v| v.level)
    }

    /// Iter ssts at given `level`.
    ///
    /// Panic if level is out of bound.
    pub fn iter_ssts_at_level(&self, level: Level) -> Iter {
        let level_handler = &self.levelHandlers[level.as_usize()];
        level_handler.iter_ssts()
    }

    pub fn collect_expired_at_level(
        &self,
        level: Level,
        expire_time: Option<Timestamp>,
    ) -> Vec<FileHandle> {
        let level_handler = &self.levelHandlers[level.as_usize()];
        let mut expired = Vec::new();
        level_handler.collect_expired(expire_time, &mut expired);

        expired
    }

    pub fn has_expired_sst(&self, expire_time: Option<Timestamp>) -> bool {
        self.levelHandlers
            .iter()
            .any(|level_handler| level_handler.has_expired_sst(expire_time))
    }

    pub fn expired_ssts(&self, expire_time: Option<Timestamp>) -> Vec<ExpiredFiles> {
        self.levels()
            .map(|level| {
                let files = self.collect_expired_at_level(level, expire_time);
                ExpiredFiles { level, files }
            })
            .collect()
    }
}