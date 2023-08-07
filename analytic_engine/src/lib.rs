// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Analytic table engine implementations

#![feature(option_get_or_insert_default)]
#![allow(non_snake_case)]
mod compaction;
mod context;
mod engine;
mod instance;
mod manifest;
pub mod memtable;
mod payload;
pub mod prefetchable_stream;
pub mod row_iter;
mod sampler;
pub mod setup;
pub mod space;
pub mod sst;
pub mod table;
pub mod table_options;
pub mod table_meta_set_impl;

use manifest::details::Options as ManifestOptions;
use message_queue::kafka::config::Config as KafkaConfig;
use object_store::config::StorageOptions;
use serde::{Deserialize, Serialize};
use size_ext::ReadableSize;
use wal::{
    message_queue_impl::config::Config as MessageQueueWalConfig,
    rocks_impl::config::Config as RocksDBWalConfig,
};

pub use crate::{compaction::scheduler::SchedulerConfig, table_options::TableOptions};


#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct AnalyticEngineConfig {
    /// storage options of the engine
    pub storage: StorageOptions,

    /// Batch size to read records from wal to replay
    pub replay_batch_size: usize,
    /// Batch size to replay tables
    pub max_replay_tables_per_batch: usize,

    /// Default options for table
    pub table_opts: TableOptions,

    pub compaction: SchedulerConfig,

    /// sst meta cache capacity
    pub sst_meta_cache_cap: Option<usize>,
    /// sst data cache capacity
    pub sst_data_cache_cap: Option<usize>,

    /// Manifest options
    pub manifest: ManifestOptions,

    /// The maximum rows in the write queue.
    pub max_rows_in_write_queue: usize,
    /// The maximum write buffer size used for single space.
    pub space_write_buffer_size: usize,
    /// The maximum size of all Write Buffers across all spaces.
    pub db_write_buffer_size: usize,
    /// The ratio of table's write buffer size to trigger preflush, and it
    /// should be in the range (0, 1].
    pub preflush_write_buffer_size_ratio: f32,

    // Iterator scanning options
    /// Batch size for iterator.
    ///
    /// The `num_rows_per_row_group` in `table options` will be used if this is
    /// not set.
    pub scan_batch_size: Option<usize>,
    /// Max record batches in flight when scan
    pub scan_max_record_batches_in_flight: usize,
    /// Sst background reading parallelism
    pub sst_background_read_parallelism: usize,
    /// Number of streams to prefetch
    pub num_streams_to_prefetch: usize,
    /// Max buffer size for writing sst
    pub write_sst_max_buffer_size: ReadableSize,
    /// Max retry limit After flush failed
    pub max_retry_flush_limit: usize,
    /// Max bytes per write batch.
    ///
    /// If this is set, the atomicity of write request will be broken.
    pub max_bytes_per_write_batch: Option<ReadableSize>,

    /// Wal storage config 默认rocksdb
    ///
    /// Now, following three storages are supported:
    /// + RocksDB
    /// + OBKV
    /// + Kafka
    pub walStorageConfig: WalStorageConfig,

    /// Recover mode 默认是tableBased
    ///
    /// + TableBased, tables on same shard will be recovered table by table.
    /// + ShardBased, tables on same shard will be recovered together.
    pub recover_mode: RecoverMode,

    pub remote_engine_client: remote_engine_client::config::Config,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum RecoverMode {
    TableBased,
    ShardBased,
}

impl Default for AnalyticEngineConfig {
    fn default() -> Self {
        Self {
            storage: Default::default(),
            replay_batch_size: 500,
            max_replay_tables_per_batch: 64,
            table_opts: TableOptions::default(),
            compaction: SchedulerConfig::default(),
            sst_meta_cache_cap: Some(1000),
            sst_data_cache_cap: Some(1000),
            manifest: ManifestOptions::default(),
            max_rows_in_write_queue: 0,
            /// Zero means disabling this param, give a positive value to enable it
            space_write_buffer_size: 0,
            /// Zero means disabling this param, give a positive value to enable it
            db_write_buffer_size: 0,
            preflush_write_buffer_size_ratio: 0.75,
            scan_batch_size: None,
            sst_background_read_parallelism: 8,
            num_streams_to_prefetch: 2,
            scan_max_record_batches_in_flight: 1024,
            write_sst_max_buffer_size: ReadableSize::mb(10),
            max_retry_flush_limit: 0,
            max_bytes_per_write_batch: None,
            walStorageConfig: WalStorageConfig::RocksDB(Box::default()),
            remote_engine_client: remote_engine_client::config::Config::default(),
            recover_mode: RecoverMode::TableBased,
        }
    }
}

/// Config of wal based on obkv
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct KafkaWalConfig {
    /// Kafka client config
    pub kafka: KafkaConfig,

    /// Namespace config for data.
    pub data_namespace: MessageQueueWalConfig,
    /// Namespace config for meta data
    pub meta_namespace: MessageQueueWalConfig,
}

/// Config for wal based on RocksDB
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct RocksDBConfig {
    /// data directory used by RocksDB 默认在$HOME/ceresdb_wal_rocksdb
    pub data_dir: String,

    pub data_namespace: RocksDBWalConfig,
    pub meta_namespace: RocksDBWalConfig,
}

impl Default for RocksDBConfig {
    fn default() -> Self {
        Self {
            data_dir: format!("{}{}",env!("HOME"),"/ceresdb_wal_rocksdb"),
            data_namespace: Default::default(),
            meta_namespace: Default::default(),
        }
    }
}

/// Options for wal storage backend
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum WalStorageConfig {
    RocksDB(Box<RocksDBConfig>),
    Kafka(Box<KafkaWalConfig>),
}
