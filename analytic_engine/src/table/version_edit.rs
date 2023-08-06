// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Version edits

use std::convert::TryFrom;

use ceresdbproto::manifest as manifest_pb;
use common_types::{time::TimeRange, SequenceNumber};
use macros::define_result;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};

use crate::{
    sst::{
        file::{FileMeta, Level},
        manager::FileId,
    },
    table::data::MemTableId,
    table_options::StorageFormat,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid level:{}, err:{}.\nBacktrace:\n{}", level, source, backtrace))]
    InvalidLevel {
        level: u32,
        source: std::num::TryFromIntError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert time range, err:{}", source))]
    ConvertTimeRange { source: common_types::time::Error },

    #[snafu(display("Fail to convert table schema, err:{}", source))]
    ConvertTableSchema { source: common_types::schema::Error },

    #[snafu(display("Time range is not found.\nBacktrace:\n{}", backtrace))]
    TimeRangeNotFound { backtrace: Backtrace },

    #[snafu(display("Table schema is not found.\nBacktrace:\n{}", backtrace))]
    TableSchemaNotFound { backtrace: Backtrace },
}

define_result!(Error);

/// Meta data of a new file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddFile {
    /// The level of the file intended to add.
    pub level: Level,
    /// Meta data of the file to add.
    pub file: FileMeta,
}

impl From<AddFile> for manifest_pb::AddFileMeta {
    /// Convert into protobuf struct
    fn from(v: AddFile) -> manifest_pb::AddFileMeta {
        manifest_pb::AddFileMeta {
            level: v.level.as_u32(),
            file_id: v.file.id,
            time_range: Some(v.file.time_range.into()),
            max_seq: v.file.max_seq,
            size: v.file.size,
            row_num: v.file.row_num,
            storage_format: manifest_pb::StorageFormat::from(v.file.storage_format) as i32,
        }
    }
}

impl TryFrom<manifest_pb::AddFileMeta> for AddFile {
    type Error = Error;

    fn try_from(src: manifest_pb::AddFileMeta) -> Result<Self> {
        let storage_format = src.storage_format();
        let time_range = {
            let time_range = src.time_range.context(TimeRangeNotFound)?;
            TimeRange::try_from(time_range).context(ConvertTimeRange)?
        };

        let target = Self {
            level: (src.level as u16).into(),
            file: FileMeta {
                id: src.file_id,
                size: src.size,
                row_num: src.row_num,
                time_range,
                max_seq: src.max_seq,
                storage_format: StorageFormat::from(storage_format),
            },
        };

        Ok(target)
    }
}

/// Meta data of the file to delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteFile {
    /// The level of the file intended to delete.
    pub level: Level,
    /// Id of the file to delete.
    pub file_id: FileId,
}

impl From<DeleteFile> for manifest_pb::DeleteFileMeta {
    fn from(v: DeleteFile) -> Self {
        manifest_pb::DeleteFileMeta {
            level: v.level.as_u32(),
            file_id: v.file_id,
        }
    }
}

impl TryFrom<manifest_pb::DeleteFileMeta> for DeleteFile {
    type Error = Error;

    fn try_from(src: manifest_pb::DeleteFileMeta) -> Result<Self> {
        let level = (src.level as u16).into();

        Ok(Self {
            level,
            file_id: src.file_id,
        })
    }
}

/// Edit to the [TableVersion], which should be done atomically
#[derive(Debug)]
pub struct VersionEdit {
    /// The last sequence already flushed. This field is not guaranteed to be
    /// set if the version edit is created by a non-flush operation (such as
    /// compaction).
    pub flushed_sequence: SequenceNumber,
    /// Id of memtables to remove from immutable memtable lists.
    pub mems_to_remove: Vec<MemTableId>,
    /// Sst files to add.
    pub files_to_add: Vec<AddFile>,
    /// Sst files to delete.
    pub files_to_delete: Vec<DeleteFile>,
    pub max_file_id: FileId,
}