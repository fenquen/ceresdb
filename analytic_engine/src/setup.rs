// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Setup the analytic engine

use std::{num::NonZeroUsize, path::Path, pin::Pin, sync::Arc};

use async_trait::async_trait;
use futures::Future;
use macros::define_result;
use message_queue::kafka::kafka_impl::KafkaImpl;
use object_store::{aliyun, config::{ObjectStoreOptions, StorageOptions}, disk_cache::ObjectStoreWithDiskCache, mem_cache::{MemCache, ObjectStoreWithMemCache}, metrics::ObjectStoreWithMetrics, prefix::StoreWithPrefix, s3, LocalFileSystem, ObjectStoreRef, ObjectStore};
use snafu::{Backtrace, ResultExt, Snafu};
use table_engine::engine::{EngineRuntimes, TableEngineRef};
use table_kv::{memory::MemoryImpl, TableKv};
use wal::{
    manager::{self, WalManagerRef},
    message_queue_impl::wal::MessageQueueImpl,
    rocks_impl::manager::Builder as RocksWalBuilder,
};
use wal::manager::WalManager;

use crate::{
    context::OpenedTableEngineInstanceContext,
    engine::AnalyticTableEngine,
    instance::{open::ManifestStorages, TableEngineInstance, InstanceRef},
    sst::{
        factory::{SstFactoryImpl, ObjectStoreChooser, ReadFrequency},
        meta_data::cache::{MetaCache, MetaCacheRef},
    },
    AnalyticEngineConfig, WalStorageConfig,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to open engine instance, err:{}", source))]
    OpenInstance {
        source: crate::instance::engine::Error,
    },

    #[snafu(display("Failed to open wal, err:{}", source))]
    OpenWal { source: manager::error::Error },

    #[snafu(display(
    "Failed to open with the invalid config, msg:{}.\nBacktrace:\n{}",
    msg,
    backtrace
    ))]
    InvalidWalConfig { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to open wal for manifest, err:{}", source))]
    OpenManifestWal { source: manager::error::Error },

    #[snafu(display("Failed to open manifest, err:{}", source))]
    OpenManifest { source: crate::manifest::details::Error },

    #[snafu(display("Failed to execute in runtime, err:{}", source))]
    RuntimeExec { source: runtime::Error },

    #[snafu(display("Failed to open object store, err:{}", source))]
    OpenObjectStore {
        source: object_store::ObjectStoreError,
    },

    #[snafu(display("Failed to create dir for {}, err:{}", path, source))]
    CreateDir {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Failed to open kafka, err:{}", source))]
    OpenKafka {
        source: message_queue::kafka::kafka_impl::Error,
    },

    #[snafu(display("Failed to create mem cache, err:{}", source))]
    OpenMemCache {
        source: object_store::mem_cache::Error,
    },
}

define_result!(Error);

const WAL_DIR_NAME: &str = "wal";
const MANIFEST_DIR_NAME: &str = "manifest";
const STORE_DIR_NAME: &str = "store";
const DISK_CACHE_DIR_NAME: &str = "sst_cache";

/// Builder for [TableEngine].
///
/// [TableEngine]: table_engine::engine::TableEngine
#[derive(Clone)]
pub struct AnalyticTableEngineBuilder<'a> {
    pub analyticConfig: &'a AnalyticEngineConfig,
    pub engine_runtimes: Arc<EngineRuntimes>,
    pub opened_wals: OpenedWals,
}

impl<'a> AnalyticTableEngineBuilder<'a> {
    pub async fn build(self) -> Result<TableEngineRef> {
        let openedStorages =
            open_storage(self.analyticConfig.storage.clone(),
                         self.engine_runtimes.clone()).await?;

        let manifest_storages = ManifestStorages {
            wal_manager: self.opened_wals.manifestWalManager.clone(),
            objectStore: openedStorages.chooseDefault().clone(),
        };

        let tableEngineInstance = open_instance(
            self.analyticConfig.clone(),
            self.engine_runtimes,
            self.opened_wals.dataWalManager,
            manifest_storages,
            Arc::new(openedStorages),
        ).await?;

        Ok(Arc::new(AnalyticTableEngine::new(tableEngineInstance)))
    }
}

#[derive(Debug, Clone)]
pub struct OpenedWals {
    pub dataWalManager: Arc<dyn WalManager>,
    pub manifestWalManager: Arc<dyn WalManager>,
}

/// Analytic engine builder.
#[async_trait]
pub trait WalsOpener: Send + Sync + Default {
    async fn open_wals(&self,
                       walStorageConfig: &WalStorageConfig,
                       engine_runtimes: Arc<EngineRuntimes>) -> Result<OpenedWals>;
}

/// [RocksEngine] builder.
#[derive(Default)]
pub struct RocksDBWalsOpener;

#[async_trait]
impl WalsOpener for RocksDBWalsOpener {
    async fn open_wals(&self,
                       config: &WalStorageConfig,
                       engine_runtimes: Arc<EngineRuntimes>) -> Result<OpenedWals> {
        let rocksdb_wal_config = match &config {
            WalStorageConfig::RocksDB(config) => config.clone(),
            _ => {
                return InvalidWalConfig {
                    msg: format!(
                        "invalid wal storage config while opening rocksDB wal, config:{config:?}"
                    ),
                }
                    .fail();
            }
        };

        let write_runtime = engine_runtimes.write_runtime.clone();
        let rocksDbDirPath = Path::new(&rocksdb_wal_config.data_dir);

        let dataPath = rocksDbDirPath.join(WAL_DIR_NAME);
        let data_wal = RocksWalBuilder::new(dataPath, write_runtime.clone())
            .max_subcompactions(rocksdb_wal_config.data_namespace.max_subcompactions)
            .max_background_jobs(rocksdb_wal_config.data_namespace.max_background_jobs)
            .enable_statistics(rocksdb_wal_config.data_namespace.enable_statistics)
            .write_buffer_size(rocksdb_wal_config.data_namespace.write_buffer_size.0)
            .max_write_buffer_number(rocksdb_wal_config.data_namespace.max_write_buffer_number)
            .level_zero_file_num_compaction_trigger(
                rocksdb_wal_config
                    .data_namespace
                    .level_zero_file_num_compaction_trigger,
            )
            .level_zero_slowdown_writes_trigger(
                rocksdb_wal_config
                    .data_namespace
                    .level_zero_slowdown_writes_trigger,
            )
            .level_zero_stop_writes_trigger(
                rocksdb_wal_config
                    .data_namespace
                    .level_zero_stop_writes_trigger,
            )
            .fifo_compaction_max_table_files_size(
                rocksdb_wal_config
                    .data_namespace
                    .fifo_compaction_max_table_files_size
                    .0,
            )
            .build()
            .context(OpenWal)?;

        let manifest_path = rocksDbDirPath.join(MANIFEST_DIR_NAME);
        let manifest_wal = RocksWalBuilder::new(manifest_path, write_runtime)
            .max_subcompactions(rocksdb_wal_config.meta_namespace.max_subcompactions)
            .max_background_jobs(rocksdb_wal_config.meta_namespace.max_background_jobs)
            .enable_statistics(rocksdb_wal_config.meta_namespace.enable_statistics)
            .write_buffer_size(rocksdb_wal_config.meta_namespace.write_buffer_size.0)
            .max_write_buffer_number(rocksdb_wal_config.meta_namespace.max_write_buffer_number)
            .level_zero_file_num_compaction_trigger(
                rocksdb_wal_config
                    .meta_namespace
                    .level_zero_file_num_compaction_trigger,
            )
            .level_zero_slowdown_writes_trigger(
                rocksdb_wal_config
                    .meta_namespace
                    .level_zero_slowdown_writes_trigger,
            )
            .level_zero_stop_writes_trigger(
                rocksdb_wal_config
                    .meta_namespace
                    .level_zero_stop_writes_trigger,
            )
            .fifo_compaction_max_table_files_size(
                rocksdb_wal_config
                    .meta_namespace
                    .fifo_compaction_max_table_files_size
                    .0,
            )
            .build()
            .context(OpenManifestWal)?;

        let opened_wals = OpenedWals {
            dataWalManager: Arc::new(data_wal),
            manifestWalManager: Arc::new(manifest_wal),
        };

        Ok(opened_wals)
    }
}

/// [MemWalEngine] builder.
///
/// All engine built by this builder share same [MemoryImpl] instance, so the
/// data wrote by the engine still remains after the engine dropped.
#[derive(Default)]
pub struct MemWalsOpener {
    table_kv: MemoryImpl,
}

#[async_trait]
impl WalsOpener for MemWalsOpener {
    async fn open_wals(&self,
                       config: &WalStorageConfig,
                       engine_runtimes: Arc<EngineRuntimes>) -> Result<OpenedWals> {
        let obkv_wal_config = match config {
            _ => {
                return InvalidWalConfig {
                    msg: format!(
                        "invalid wal storage config while opening memory wal, config:{config:?}"
                    ),
                }
                    .fail();
            }
        };
    }
}

#[derive(Default)]
pub struct KafkaWalsOpener;

#[async_trait]
impl WalsOpener for KafkaWalsOpener {
    async fn open_wals(&self,
                       config: &WalStorageConfig,
                       engine_runtimes: Arc<EngineRuntimes>) -> Result<OpenedWals> {
        let kafka_wal_config = match config {
            WalStorageConfig::Kafka(config) => config.clone(),
            _ => {
                return InvalidWalConfig { msg: format!("invalid wal storage config while opening kafka wal, config:{config:?}")}.fail();
            }
        };

        let default_runtime = &engine_runtimes.default_runtime;

        let kafka = KafkaImpl::new(kafka_wal_config.kafka.clone()).await.context(OpenKafka)?;

        let data_wal = MessageQueueImpl::new(
            WAL_DIR_NAME.to_string(),
            kafka.clone(),
            default_runtime.clone(),
            kafka_wal_config.data_namespace,
        );

        let manifest_wal = MessageQueueImpl::new(
            MANIFEST_DIR_NAME.to_string(),
            kafka,
            default_runtime.clone(),
            kafka_wal_config.meta_namespace,
        );

        Ok(OpenedWals {
            dataWalManager: Arc::new(data_wal),
            manifestWalManager: Arc::new(manifest_wal),
        })
    }
}

async fn open_instance(analyticConfig: AnalyticEngineConfig,
                       engine_runtimes: Arc<EngineRuntimes>,
                       wal_manager: WalManagerRef,
                       manifest_storages: ManifestStorages,
                       store_picker: Arc<dyn ObjectStoreChooser>) -> Result<InstanceRef> {
    let meta_cache: Option<MetaCacheRef> = analyticConfig.sst_meta_cache_cap.map(|cap| Arc::new(MetaCache::new(cap)));

    let open_ctx = OpenedTableEngineInstanceContext {
        analyticEngineConfig: analyticConfig,
        engineRuntimes: engine_runtimes,
        meta_cache,
    };

    let tableEngineInstance =
        TableEngineInstance::open(open_ctx,
                                  manifest_storages,
                                  wal_manager,
                                  store_picker,
                                  Arc::new(SstFactoryImpl::default())).await.context(OpenInstance)?;

    Ok(tableEngineInstance)
}

#[derive(Debug)]
struct OpenedStorages {
    /// memCacheStore -> objectStoreWithMetrics -> objectStoreWithDiskCache  localFileSystem
    defaultStore: Arc<dyn ObjectStore>,
    storeWithReadCache: Arc<dyn ObjectStore>,
}

impl ObjectStoreChooser for OpenedStorages {
    fn chooseDefault(&self) -> &ObjectStoreRef {
        &self.defaultStore
    }

    fn chooseByReadFrequency(&self, freq: ReadFrequency) -> &ObjectStoreRef {
        match freq {
            ReadFrequency::Once => &self.storeWithReadCache,
            ReadFrequency::Frequent => &self.defaultStore,
        }
    }
}

// Build store in multiple layer, access speed decrease in turn.
// MemCacheStore           → DiskCacheStore → real ObjectStore(OSS/S3...)
// MemCacheStore(ReadOnly) ↑
// ```plaintext
// +-------------------------------+
// |    MemCacheStore              |
// |       +-----------------------+
// |       |    DiskCacheStore     |
// |       |      +----------------+
// |       |      |                |
// |       |      |    OSS/S3....  |
// +-------+------+----------------+
// ```
fn open_storage(storageOptions: StorageOptions,
                engineRuntimes: Arc<EngineRuntimes>) -> Pin<Box<dyn Future<Output=Result<OpenedStorages>> + Send>> {
    Box::pin(async move {
        let mut objectStore = match storageOptions.objectStoreOptions {
            ObjectStoreOptions::Local(local_opts) => {
                let sst_path = Path::new(&local_opts.data_dir).join(STORE_DIR_NAME);
                tokio::fs::create_dir_all(&sst_path).await.context(CreateDir { path: sst_path.to_string_lossy().into_owned() })?;
                let localFileSystem = LocalFileSystem::new_with_prefix(sst_path).context(OpenObjectStore)?;
                Arc::new(localFileSystem) as _
            }
            ObjectStoreOptions::Aliyun(aliyun_opts) => {
                let oss: ObjectStoreRef =
                    Arc::new(aliyun::try_new(&aliyun_opts).context(OpenObjectStore)?);
                let store_with_prefix = StoreWithPrefix::new(aliyun_opts.prefix, oss);
                Arc::new(store_with_prefix.context(OpenObjectStore)?) as _
            }
            ObjectStoreOptions::S3(s3_option) => {
                let oss: ObjectStoreRef =
                    Arc::new(s3::try_new(&s3_option).context(OpenObjectStore)?);
                let store_with_prefix = StoreWithPrefix::new(s3_option.prefix, oss);
                Arc::new(store_with_prefix.context(OpenObjectStore)?) as _
            }
        };

        // 以下类似了java的装饰器模式在原来的基地上不断增加能力

        // 增强成 ObjectStoreWithDiskCache
        // 默认是0
        if storageOptions.disk_cache_capacity.as_byte() > 0 {
            let path = Path::new(&storageOptions.disk_cache_dir).join(DISK_CACHE_DIR_NAME);
            tokio::fs::create_dir_all(&path).await.context(CreateDir {
                path: path.to_string_lossy().into_owned(),
            })?;

            objectStore = Arc::new(
                ObjectStoreWithDiskCache::try_new(
                    path.to_string_lossy().into_owned(),
                    storageOptions.disk_cache_capacity.as_byte() as usize,
                    storageOptions.disk_cache_page_size.as_byte() as usize,
                    objectStore,
                    storageOptions.disk_cache_partition_bits,
                ).await.context(OpenObjectStore)?,
            ) as _;
        }

        // 增强成 ObjectStoreWithMemCache
        objectStore = Arc::new(ObjectStoreWithMetrics::new(objectStore, engineRuntimes.io_runtime.clone()));

        // 增强成 ObjectStoreWithMemCache
        // 默认512
        if storageOptions.mem_cache_capacity.as_byte() > 0 {
            let mem_cache = Arc::new(
                MemCache::try_new(
                    storageOptions.mem_cache_partition_bits,
                    NonZeroUsize::new(storageOptions.mem_cache_capacity.as_byte() as usize).unwrap(),
                ).context(OpenMemCache)?,
            );

            let default_store = Arc::new(ObjectStoreWithMemCache::new(mem_cache.clone(), objectStore.clone())) as _;
            let store_with_readonly_cache = Arc::new(ObjectStoreWithMemCache::new_with_readonly_cache(mem_cache, objectStore)) as _;
            Ok(OpenedStorages {
                defaultStore: default_store,
                storeWithReadCache: store_with_readonly_cache,
            })
        } else {
            let a = objectStore.clone();
            Ok(OpenedStorages {
                defaultStore: objectStore,
                storeWithReadCache: a,
            })
        }
    })
}
