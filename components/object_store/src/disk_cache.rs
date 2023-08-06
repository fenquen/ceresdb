// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! An ObjectStore implementation with disk as cache.
//! The disk cache is a read-through caching, with page as its minimal cache
//! unit.
//!
//! Page is used for reasons below:
//! - reduce file size in case of there are too many request with small range.

use std::{collections::BTreeMap, fmt::Display, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use crc::{Crc, CRC_32_ISCSI};
use futures::stream::BoxStream;
use hash_ext::SeaHasherBuilder;
use log::{debug, error, info};
use lru::LruCache;
use partitioned_lock::PartitionedMutexAsync;
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use time_ext;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::Mutex,
};
use upstream::{
    path::Path, Error as ObjectStoreError, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result,
};

const MANIFEST_FILE: &str = "manifest.json";
const CURRENT_VERSION: usize = 1;
pub const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display(
    "IO failed, file:{}, source:{}.\nbacktrace:\n{}",
    file,
    source,
    backtrace
    ))]
    Io {
        file: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Failed to deserialize manifest, source:{}.\nbacktrace:\n{}",
    source,
    backtrace
    ))]
    DeserializeManifest {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Failed to serialize manifest, source:{}.\nbacktrace:\n{}",
    source,
    backtrace
    ))]
    SerializeManifest {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid manifest page size, old:{}, new:{}.", old, new))]
    InvalidManifest { old: usize, new: usize },

    #[snafu(display(
    "Failed to persist cache, file:{}, source:{}.\nbacktrace:\n{}",
    file,
    source,
    backtrace
    ))]
    PersistCache {
        file: String,
        source: tokio::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Failed to decode cache pb value, file:{}, source:{}.\nbacktrace:\n{}",
    file,
    source,
    backtrace
    ))]
    DecodeCache {
        file: String,
        source: prost::DecodeError,
        backtrace: Backtrace,
    },
    #[snafu(display("disk cache cap must large than 0", ))]
    InvalidCapacity,
}

impl From<Error> for ObjectStoreError {
    fn from(source: Error) -> Self {
        Self::Generic {
            store: "DiskCacheStore",
            source: Box::new(source),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    create_at: String,
    page_size: usize,
    version: usize,
}

// TODO: support partition to reduce lock contention.
#[derive(Debug)]
struct DiskCache {
    root_dir: String,
    // Cache key is used as filename on disk.
    cache: PartitionedMutexAsync<LruCache<String, ()>, SeaHasherBuilder>,
}

impl DiskCache {
    fn try_new(root_dir: String, cap: usize, partition_bits: usize) -> Result<Self> {
        let init_lru = |partition_num: usize| -> Result<_> {
            let cap_per_part = cap / partition_num;
            ensure!(cap_per_part != 0, InvalidCapacity);
            Ok(LruCache::new(cap_per_part))
        };

        Ok(Self {
            root_dir,
            cache: PartitionedMutexAsync::try_new(init_lru, partition_bits, SeaHasherBuilder {})?,
        })
    }

    /// Update the cache.
    ///
    /// The returned value denotes whether succeed.
    // TODO: We now hold lock when doing IO, possible to release it?
    async fn update_cache(&self, key: String, value: Option<Bytes>) -> bool {
        let mut cache = self.cache.lock(&key).await;
        debug!(
            "Disk cache update, key:{}, len:{}, cap_per_part:{}.",
            &key,
            cache.cap(),
            cache.len(),
        );

        // TODO: remove a batch of files to avoid IO during the following update cache.
        if cache.len() >= cache.cap() {
            let (filename, _) = cache.pop_lru().unwrap();
            let file_path = std::path::Path::new(&self.root_dir)
                .join(filename)
                .into_os_string()
                .into_string()
                .unwrap();

            debug!("Remove disk cache, filename:{}.", &file_path);
            if let Err(e) = tokio::fs::remove_file(&file_path).await {
                error!("Remove disk cache failed, file:{}, err:{}.", file_path, e);
            }
        }

        // Persist the value if needed
        if let Some(value) = value {
            if let Err(e) = self.persist_bytes(&key, value).await {
                error!("Failed to persist cache, key:{}, err:{}.", key, e);
                return false;
            }
        }

        // Update the key
        cache.push(key, ());

        true
    }

    async fn insert(&self, key: String, value: Bytes) -> bool {
        self.update_cache(key, Some(value)).await
    }

    async fn recover(&self, filename: String) -> bool {
        self.update_cache(filename, None).await
    }

    async fn get(&self, key: &str) -> Option<Bytes> {
        let mut cache = self.cache.lock(&key).await;
        if cache.get(key).is_some() {
            // TODO: release lock when doing IO
            match self.read_bytes(key).await {
                Ok(v) => Some(v),
                Err(e) => {
                    error!(
                        "Read disk cache failed but ignored, key:{}, err:{}.",
                        key, e
                    );
                    None
                }
            }
        } else {
            None
        }
    }

    async fn persist_bytes(&self, filename: &str, value: Bytes) -> Result<()> {
        let file_path = std::path::Path::new(&self.root_dir)
            .join(filename)
            .into_os_string()
            .into_string()
            .unwrap();

        let mut file = File::create(&file_path).await.with_context(|| Io {
            file: file_path.clone(),
        })?;

        let bytes = value.to_vec();
        let pb_bytes = ceresdbproto::oss_cache::Bytes {
            crc: CASTAGNOLI.checksum(&bytes),
            value: bytes,
        };

        file.write_all(&pb_bytes.encode_to_vec())
            .await
            .with_context(|| PersistCache {
                file: file_path.clone(),
            })?;

        Ok(())
    }

    async fn read_bytes(&self, filename: &str) -> Result<Bytes> {
        let file_path = std::path::Path::new(&self.root_dir)
            .join(filename)
            .into_os_string()
            .into_string()
            .unwrap();

        let mut f = File::open(&file_path).await.with_context(|| Io {
            file: file_path.clone(),
        })?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).await.with_context(|| Io {
            file: file_path.clone(),
        })?;

        let bytes = ceresdbproto::oss_cache::Bytes::decode(&*buf).with_context(|| DecodeCache {
            file: file_path.clone(),
        })?;
        // TODO: CRC checking

        Ok(bytes.value.into())
    }
}

impl Display for DiskCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCache")
            .field("path", &self.root_dir)
            .field("cache", &self.cache)
            .finish()
    }
}

/// There will be two kinds of file in this cache:
/// 1. manifest.json, which contains metadata, like
/// ```json
/// {
///     "create_at": "2022-12-01T08:51:15.167795+00:00",
///     "page_size": 1048576,
///     "version": 1
/// }
/// ```
/// 2. ${sst-path}-${range.start}-${range.end}, which contains bytes of given
/// range, start/end are aligned to page_size.
#[derive(Debug)]
pub struct ObjectStoreWithDiskCache {
    cache: DiskCache,
    // Max disk capacity cache use can
    cap: usize,
    // Size of each cached bytes
    page_size: usize,
    // location path size cache
    size_cache: Arc<Mutex<LruCache<String, usize>>>,
    underlyingObjectStore: Arc<dyn ObjectStore>,
}

impl ObjectStoreWithDiskCache {
    pub async fn try_new(cache_dir: String,
                         cap: usize,
                         page_size: usize,
                         underlying_store: Arc<dyn ObjectStore>,
                         partition_bits: usize, ) -> Result<Self> {
        let page_num_per_part = cap / page_size;
        ensure!(page_num_per_part != 0, InvalidCapacity);
        let _ = Self::create_manifest_if_not_exists(&cache_dir, page_size).await?;
        let cache = DiskCache::try_new(cache_dir.clone(), page_num_per_part, partition_bits)?;
        Self::recover_cache(&cache_dir, &cache).await?;

        let size_cache = Arc::new(Mutex::new(LruCache::new(cap / page_size)));

        Ok(Self {
            cache,
            size_cache,
            cap,
            page_size,
            underlyingObjectStore: underlying_store,
        })
    }

    async fn create_manifest_if_not_exists(cache_dir: &str, page_size: usize) -> Result<Manifest> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .truncate(false)
            .open(std::path::Path::new(cache_dir).join(MANIFEST_FILE))
            .await
            .with_context(|| Io {
                file: MANIFEST_FILE.to_string(),
            })?;

        let metadata = file.metadata().await.with_context(|| Io {
            file: MANIFEST_FILE.to_string(),
        })?;

        // empty file, create a new one
        if metadata.len() == 0 {
            let manifest = Manifest {
                page_size,
                create_at: time_ext::current_as_rfc3339(),
                version: CURRENT_VERSION,
            };

            let buf = serde_json::to_vec_pretty(&manifest).context(SerializeManifest)?;
            file.write_all(&buf).await.with_context(|| Io {
                file: MANIFEST_FILE.to_string(),
            })?;

            return Ok(manifest);
        }

        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await.with_context(|| Io {
            file: MANIFEST_FILE.to_string(),
        })?;

        let manifest: Manifest = serde_json::from_slice(&buf).context(DeserializeManifest)?;

        ensure!(
            manifest.page_size == page_size,
            InvalidManifest {
                old: manifest.page_size,
                new: page_size
            }
        );
        // TODO: check version

        Ok(manifest)
    }

    async fn recover_cache(cache_dir: &str, cache: &DiskCache) -> Result<()> {
        let mut cache_dir = tokio::fs::read_dir(cache_dir).await.with_context(|| Io {
            file: cache_dir.to_string(),
        })?;

        // TODO: sort by access time
        while let Some(entry) = cache_dir.next_entry().await.with_context(|| Io {
            file: "entry when iter cache_dir".to_string(),
        })? {
            let filename = entry.file_name().into_string().unwrap();
            info!("Disk cache recover_cache, filename:{}.", &filename);

            if filename != MANIFEST_FILE {
                cache.recover(filename).await;
            }
        }

        Ok(())
    }

    fn normalize_range(&self, max_size: usize, range: &Range<usize>) -> Vec<Range<usize>> {
        let start = range.start / self.page_size * self.page_size;
        let end = (range.end + self.page_size - 1) / self.page_size * self.page_size;

        (start..end.min(max_size))
            .step_by(self.page_size)
            .map(|start| start..(start + self.page_size).min(max_size))
            .collect::<Vec<_>>()
    }

    fn cache_key(location: &Path, range: &Range<usize>) -> String {
        format!(
            "{}-{}-{}",
            location.as_ref().replace('/', "-"),
            range.start,
            range.end
        )
    }
}

impl Display for ObjectStoreWithDiskCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCacheStore")
            .field("page_size", &self.page_size)
            .field("cap", &self.cap)
            .field("cache", &self.cache)
            .finish()
    }
}

#[async_trait]
impl ObjectStore for ObjectStoreWithDiskCache {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.underlyingObjectStore.put(location, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.underlyingObjectStore.put_multipart(location).await
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        self.underlyingObjectStore
            .abort_multipart(location, multipart_id)
            .await
    }

    // TODO: don't cache whole path for reasons below
    // In sst module, we only use get_range, get is not used
    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.underlyingObjectStore.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        // TODO: aligned_range will larger than real file size, need to truncate
        let file_size = {
            let mut size_cache = self.size_cache.lock().await;
            if let Some(size) = size_cache.get(location.as_ref()) {
                *size
            } else {
                // release lock before doing IO
                drop(size_cache);

                // TODO: multiple threads may go here, how to fix?
                let object_meta = self.head(location).await?;
                {
                    let mut size_cache = self.size_cache.lock().await;
                    size_cache.put(location.to_string(), object_meta.size);
                }
                object_meta.size
            }
        };

        let aligned_ranges = self.normalize_range(file_size, &range);

        let mut ranged_bytes = BTreeMap::new();
        let mut missing_ranges = Vec::new();
        for range in aligned_ranges {
            let cache_key = Self::cache_key(location, &range);
            if let Some(bytes) = self.cache.get(&cache_key).await {
                ranged_bytes.insert(range.start, bytes);
            } else {
                missing_ranges.push(range);
            }
        }

        for range in missing_ranges {
            let range_start = range.start;
            let cache_key = Self::cache_key(location, &range);
            // TODO: we should use get_ranges here.
            let bytes = self.underlyingObjectStore.get_range(location, range).await?;
            self.cache.insert(cache_key, bytes.clone()).await;
            ranged_bytes.insert(range_start, bytes);
        }

        // we get all bytes for each aligned_range, organize real bytes
        // fast path
        if ranged_bytes.len() == 1 {
            let (range_start, bytes) = ranged_bytes.pop_first().unwrap();
            let result = bytes.slice((range.start - range_start)..(range.end - range_start));
            return Ok(result);
        }

        // there are multiple aligned ranges for one request, such as
        // range = [3, 33), page_size = 16, then aligned ranges will be
        // [0, 16), [16, 32), [32, 48)
        // we need to combine those ranged bytes to get final result bytes

        let mut byte_buf = BytesMut::with_capacity(range.end - range.start);
        let (range_start, bytes) = ranged_bytes.pop_first().unwrap();
        byte_buf.extend(bytes.slice((range.start - range_start)..));
        let (range_start, bytes) = ranged_bytes.pop_last().unwrap();
        let last_part = bytes.slice(..(range.end - range_start));

        for bytes in ranged_bytes.into_values() {
            byte_buf.extend(bytes);
        }
        byte_buf.extend(last_part);

        Ok(byte_buf.freeze())
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.underlyingObjectStore.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.underlyingObjectStore.delete(location).await
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        self.underlyingObjectStore.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.underlyingObjectStore.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.underlyingObjectStore.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.underlyingObjectStore.copy_if_not_exists(from, to).await
    }
}