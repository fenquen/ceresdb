// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! An implementation of ObjectStore, which support
//! 1. Cache based on memory, and support evict based on memory usage
//! 2. Builtin Partition to reduce lock contention

use std::{
    fmt::{self, Display},
    num::NonZeroUsize,
    ops::Range,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use clru::{CLruCache, CLruCacheConfig, WeightScale};
use futures::stream::BoxStream;
use hash_ext::{ahash::RandomState, build_fixed_seed_ahasher_builder};
use macros::define_result;
use partitioned_lock::PartitionedMutex;
use snafu::{OptionExt, Snafu};
use tokio::io::AsyncWrite;
use upstream::{
    path::Path, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    Result as ObjectStoreResult,
};

use crate::ObjectStoreRef;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("mem cache cap must large than 0", ))]
    InvalidCapacity,
}

define_result!(Error);

struct CustomScale;

impl WeightScale<String, Bytes> for CustomScale {
    fn weight(&self, _key: &String, value: &Bytes) -> usize {
        value.len()
    }
}

pub struct MemCache {
    mem_cap: NonZeroUsize,
    inner: PartitionedMutex<CLruCache<String, Bytes, RandomState, CustomScale>, RandomState>,
}

impl MemCache {
    pub fn try_new(partition_bits: usize, mem_cap: NonZeroUsize) -> Result<Self> {
        let init_lru = |partition_num: usize| -> Result<_> {
            let cap_per_part =
                NonZeroUsize::new(mem_cap.get() / partition_num).context(InvalidCapacity)?;
            Ok(CLruCache::with_config(
                CLruCacheConfig::new(cap_per_part)
                    .with_hasher(build_fixed_seed_ahasher_builder())
                    .with_scale(CustomScale),
            ))
        };

        let inner = PartitionedMutex::try_new(
            init_lru,
            partition_bits,
            build_fixed_seed_ahasher_builder(),
        )?;

        Ok(Self { mem_cap, inner })
    }

    fn get(&self, key: &str) -> Option<Bytes> {
        self.inner.lock(&key).get(key).cloned()
    }

    fn peek(&self, key: &str) -> Option<Bytes> {
        self.inner.lock(&key).peek(key).cloned()
    }

    fn insert(&self, key: String, value: Bytes) {
        // don't care error now.
        _ = self.inner.lock(&key).put_with_weight(key, value);
    }
}

impl Display for MemCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemCache")
            .field("mem_cap", &self.mem_cap)
            .field("partitions", &self.inner.get_all_partition().len())
            .finish()
    }
}

/// Assembled with [`MemCache`], the [`ObjectStoreWithMemCache`] can cache the loaded data
/// from the `underlying_store` to avoid unnecessary data loading.
///
/// With the `read_only_cache` field, caller can control whether to do caching
/// for the loaded data. BTW, all the accesses are forced to the order:
/// `cache` -> `underlying_store`.
pub struct ObjectStoreWithMemCache {
    memCache: Arc<MemCache>,
    underlyingObjectStore: Arc<dyn ObjectStore>,
    readonly: bool,
}

impl ObjectStoreWithMemCache {
    /// Create a default [`ObjectStoreWithMemCache`].
    pub fn new(memCache: Arc<MemCache>, underlyingObjectStore: ObjectStoreRef) -> Self {
        Self {
            memCache,
            underlyingObjectStore,
            readonly: false,
        }
    }

    /// Create a [`ObjectStoreWithMemCache`] with a readonly cache.
    pub fn new_with_readonly_cache(cache: Arc<MemCache>, underlying_store: ObjectStoreRef) -> Self {
        Self {
            memCache: cache,
            underlyingObjectStore: underlying_store,
            readonly: true,
        }
    }

    fn cache_key(location: &Path, range: &Range<usize>) -> String {
        format!("{}-{}-{}", location, range.start, range.end)
    }

    async fn get_range_with_rw_cache(
        &self,
        location: &Path,
        range: Range<usize>,
    ) -> ObjectStoreResult<Bytes> {
        // TODO(chenxiang): What if there are some overlapping range in cache?
        // A request with range [5, 10) can also use [0, 20) cache
        let cache_key = Self::cache_key(location, &range);
        if let Some(bytes) = self.memCache.get(&cache_key) {
            return Ok(bytes);
        }

        // TODO(chenxiang): What if two threads reach here? It's better to
        // pend one thread, and only let one to fetch data from underlying store.
        let bytes = self.underlyingObjectStore.get_range(location, range).await?;
        self.memCache.insert(cache_key, bytes.clone());

        Ok(bytes)
    }

    async fn get_range_with_ro_cache(
        &self,
        location: &Path,
        range: Range<usize>,
    ) -> ObjectStoreResult<Bytes> {
        let cache_key = Self::cache_key(location, &range);
        if let Some(bytes) = self.memCache.peek(&cache_key) {
            return Ok(bytes);
        }

        // TODO(chenxiang): What if two threads reach here? It's better to
        // pend one thread, and only let one to fetch data from underlying store.
        self.underlyingObjectStore.get_range(location, range).await
    }
}

impl Display for ObjectStoreWithMemCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.memCache.fmt(f)
    }
}

impl fmt::Debug for ObjectStoreWithMemCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemCacheStore").finish()
    }
}

#[async_trait]
impl ObjectStore for ObjectStoreWithMemCache {
    async fn put(&self, location: &Path, bytes: Bytes) -> ObjectStoreResult<()> {
        self.underlyingObjectStore.put(location, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> ObjectStoreResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.underlyingObjectStore.put_multipart(location).await
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> ObjectStoreResult<()> {
        self.underlyingObjectStore
            .abort_multipart(location, multipart_id)
            .await
    }

    // TODO(chenxiang): don't cache whole path for reasons below
    // 1. cache key don't support overlapping
    // 2. In sst module, we only use get_range, get is not used
    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        self.underlyingObjectStore.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        if self.readonly {
            self.get_range_with_ro_cache(location, range).await
        } else {
            self.get_range_with_rw_cache(location, range).await
        }
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        self.underlyingObjectStore.head(location).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.underlyingObjectStore.delete(location).await
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> ObjectStoreResult<BoxStream<'_, ObjectStoreResult<ObjectMeta>>> {
        self.underlyingObjectStore.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.underlyingObjectStore.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.underlyingObjectStore.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.underlyingObjectStore.copy_if_not_exists(from, to).await
    }
}