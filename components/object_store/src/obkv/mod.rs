// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    ops::Range,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use futures::{
    stream::{BoxStream, FuturesOrdered},
    StreamExt,
};
use generic_error::{BoxError, GenericError};
use log::debug;
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use table_kv::{ScanContext, ScanIter, TableKv, WriteBatch, WriteContext};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    time::Instant,
};
use twox_hash::XxHash64;
use upstream::{
    path::{Path, DELIMITER},
    Error as StoreError, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result,
};
use uuid::Uuid;

use crate::{
    multipart::{CloudMultiPartUpload, CloudMultiPartUploadImpl, UploadPart},
    obkv::meta::{MetaManager, ObkvObjectMeta, OBJECT_STORE_META},
};

mod meta;
mod util;

/// The object store type of obkv
pub const OBKV: &str = "OBKV";

/// Hash seed to build hasher. Modify the seed will result in different route
/// result!
const HASH_SEED: u64 = 0;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to scan data, namespace:{namespace}, err:{source}"))]
    ScanData {
        namespace: String,
        source: GenericError,
    },

    #[snafu(display("Failed to put data, path:{path}, err:{source}"))]
    PutData { path: String, source: GenericError },

    #[snafu(display("Failed to create shard table, table_name:{table_name}, err:{source}"))]
    CreateShardTable {
        table_name: String,
        source: GenericError,
    },

    #[snafu(display("Failed to read meta, path:{path}, err:{source}"))]
    ReadMeta { path: String, source: GenericError },

    #[snafu(display("Data part is not found in part_key:{part_key}. \nBacktrace:\n{backtrace}"))]
    DataPartNotFound {
        part_key: String,
        backtrace: Backtrace,
    },

    #[snafu(display("No meta found, path:{path}. \nBacktrace:\n{backtrace}"))]
    MetaNotExists { path: String, backtrace: Backtrace },

    #[snafu(display(
        "Data is too large to put, size:{size}, limit:{limit}. \nBacktrace:\n{backtrace}"
    ))]
    TooLargeData {
        size: usize,
        limit: usize,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Convert timestamp to date time fail, timestamp:{timestamp}. \nBacktrace:\n{backtrace}"
    ))]
    ConvertTimestamp {
        timestamp: i64,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "The length of data parts is inconsistent with the length of values, parts length:{part_len}, values length:{value_len} \nBacktrace:\n{backtrace}"
    ))]
    DataPartsLength {
        part_len: usize,
        value_len: usize,
        backtrace: Backtrace,
    },
}

impl<T: TableKv> MetaManager<T> {
    fn try_new(client: Arc<T>) -> std::result::Result<Self, Error> {
        create_table_if_not_exists(&client, OBJECT_STORE_META)?;
        Ok(Self { client })
    }
}

/// If table not exists, create shard table; Else, do nothing.
fn create_table_if_not_exists<T: TableKv>(
    table_kv: &Arc<T>,
    table_name: &str,
) -> std::result::Result<(), Error> {
    let table_exists = table_kv
        .table_exists(table_name)
        .box_err()
        .context(CreateShardTable { table_name })?;
    if !table_exists {
        table_kv
            .create_table(table_name)
            .box_err()
            .context(CreateShardTable { table_name })?;
    }

    Ok(())
}

#[derive(Debug, Clone)]
pub struct ShardManager {
    shard_num: usize,
    table_names: Vec<String>,
}

impl ShardManager {
    fn try_new<T: TableKv>(client: Arc<T>, shard_num: usize) -> std::result::Result<Self, Error> {
        let mut table_names = Vec::with_capacity(shard_num);

        for shard_id in 0..shard_num {
            let table_name = format!("object_store_{shard_id}");
            create_table_if_not_exists(&client, &table_name)?;
            table_names.push(table_name);
        }

        Ok(Self {
            shard_num,
            table_names,
        })
    }

    #[inline]
    pub fn pick_shard_table(&self, path: &Path) -> &str {
        let mut hasher = XxHash64::with_seed(HASH_SEED);
        path.as_ref().as_bytes().hash(&mut hasher);
        let hash = hasher.finish();
        let index = hash % (self.table_names.len() as u64);
        &self.table_names[index as usize]
    }
}

impl std::fmt::Display for ShardManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObjectStore ObkvShardManager({})", self.shard_num)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ObkvObjectStore<T> {
    /// The manager to manage shard table in obkv
    shard_manager: ShardManager,
    /// The manager to manage object store meta, which persist in obkv
    meta_manager: Arc<MetaManager<T>>,
    client: Arc<T>,
    /// The size of one object part persited in obkv
    /// It may cause problem to save huge data in one obkv value, so we
    /// need to split data into small parts.
    part_size: usize,
    /// The max size of bytes, default is 1GB
    max_object_size: usize,
    /// Maximum number of upload tasks to run concurrently
    max_upload_concurrency: usize,
}

impl<T: TableKv> std::fmt::Display for ObkvObjectStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ObkvObjectStore({:?},{:?})",
            self.client, self.shard_manager
        )?;
        Ok(())
    }
}

impl<T: TableKv> ObkvObjectStore<T> {
    pub fn try_new(
        client: Arc<T>,
        shard_num: usize,
        part_size: usize,
        max_object_size: usize,
        max_upload_concurrency: usize,
    ) -> Result<Self> {
        let shard_manager = ShardManager::try_new(client.clone(), shard_num).map_err(|source| {
            StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            }
        })?;
        let meta_manager: MetaManager<T> =
            MetaManager::try_new(client.clone()).map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;
        Ok(Self {
            shard_manager,
            meta_manager: Arc::new(meta_manager),
            client,
            part_size,
            max_object_size,
            max_upload_concurrency,
        })
    }

    #[inline]
    fn check_size(&self, bytes: &Bytes) -> std::result::Result<(), Error> {
        ensure!(
            bytes.len() < self.max_object_size,
            TooLargeData {
                size: bytes.len(),
                limit: self.max_object_size,
            }
        );

        Ok(())
    }

    #[inline]
    fn normalize_path(location: Option<&Path>) -> Path {
        if let Some(path) = location {
            if !path.as_ref().ends_with(DELIMITER) {
                return Path::from(format!("{}{DELIMITER}", path.as_ref()));
            }
            path.clone()
        } else {
            Path::from("")
        }
    }

    #[inline]
    pub fn pick_shard_table(&self, path: &Path) -> &str {
        self.shard_manager.pick_shard_table(path)
    }
}

impl<T: TableKv> ObkvObjectStore<T> {
    async fn read_meta(&self, location: &Path) -> std::result::Result<ObkvObjectMeta, Error> {
        let meta = self
            .meta_manager
            .read(location)
            .await
            .box_err()
            .context(ReadMeta {
                path: location.as_ref().to_string(),
            })?;

        if let Some(m) = meta {
            Ok(m)
        } else {
            MetaNotExists {
                path: location.as_ref().to_string(),
            }
            .fail()
        }
    }

    async fn get_internal(&self, location: &Path) -> std::result::Result<GetResult, Error> {
        let meta = self.read_meta(location).await?;
        let table_name = self.pick_shard_table(location);
        // TODO: Let table_kv provide a api `get_batch` to avoid extra IO operations.
        let mut futures = FuturesOrdered::new();
        for part_key in meta.parts {
            let client = self.client.clone();
            let table_name = table_name.to_string();
            let future = async move {
                match client.get(&table_name, part_key.as_bytes()) {
                    Ok(res) => Ok(Bytes::from(res.unwrap())),
                    Err(err) => Err(StoreError::Generic {
                        store: OBKV,
                        source: Box::new(err),
                    }),
                }
            };
            futures.push_back(future);
        }

        let boxed = futures.boxed();

        Ok(GetResult::Stream(boxed))
    }

    fn convert_datetime(&self, timestamp: i64) -> std::result::Result<DateTime<Utc>, Error> {
        let timestamp_millis_opt = Utc.timestamp_millis_opt(timestamp);
        if let Some(dt) = timestamp_millis_opt.single() {
            Ok(dt)
        } else {
            ConvertTimestamp { timestamp }.fail()
        }
    }
}

#[async_trait]
impl<T: TableKv> ObjectStore for ObkvObjectStore<T> {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let instant = Instant::now();

        self.check_size(&bytes)
            .map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;

        // Use `put_multipart` to implement `put`.
        let (_upload_id, mut multipart) = self.put_multipart(location).await?;
        multipart
            .write(&bytes)
            .await
            .map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;
        // Complete stage: flush buffer data to obkv, and save meta data
        multipart
            .shutdown()
            .await
            .map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;
        debug!(
            "ObkvObjectStore put operation, location:{location}, cost:{:?}",
            instant.elapsed()
        );
        Ok(())
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let instant = Instant::now();

        let upload_id = Uuid::new_v4().to_string();
        let table_name = self.pick_shard_table(location);

        let upload = ObkvMultiPartUpload {
            location: location.clone(),
            upload_id: upload_id.clone(),
            table_name: table_name.to_string(),
            size: AtomicU64::new(0),
            client: Arc::clone(&self.client),
            part_size: self.part_size,
            meta_manager: self.meta_manager.clone(),
        };
        let multi_part_upload =
            CloudMultiPartUpload::new(upload, self.max_upload_concurrency, self.part_size);

        debug!(
            "ObkvObjectStore put_multipart operation, location:{location}, table_name:{table_name}, cost:{:?}",
            instant.elapsed()
        );
        Ok((upload_id, Box::new(multi_part_upload)))
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        let instant = Instant::now();

        let table_name = self.pick_shard_table(location);

        // Before aborting multipart, we need to delete all data parts and meta info.
        // Here to delete data with path `location` and multipart_id.
        let scan_context: ScanContext = ScanContext {
            timeout: time::Duration::from_secs(meta::SCAN_TIMEOUT_SECS),
            batch_size: meta::SCAN_BATCH_SIZE,
        };

        let prefix = PathKeyEncoder::part_key_prefix(location, multipart_id);
        let scan_request = util::scan_request_with_prefix(prefix.as_bytes());

        let mut iter = self
            .client
            .scan(scan_context, table_name, scan_request)
            .map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;

        let mut keys = vec![];
        while iter.valid() {
            keys.push(iter.key().to_vec());
            iter.next().map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;
        }

        self.client
            .delete_batch(table_name, keys)
            .map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;

        // Here to delete meta with path `location` and multipart_id
        self.meta_manager
            .delete_with_version(location, multipart_id)
            .await
            .map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;

        debug!(
            "ObkvObjectStore abort_multipart operation, location:{location}, table_name:{table_name}, cost:{:?}",
            instant.elapsed()
        );
        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let instant = Instant::now();
        let result = self.get_internal(location).await;

        debug!(
            "ObkvObjectStore get operation, location:{location}, cost:{:?}",
            instant.elapsed()
        );
        result.box_err().map_err(|source| StoreError::NotFound {
            path: location.to_string(),
            source,
        })
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let instant = Instant::now();

        let table_name = self.pick_shard_table(location);
        let meta =
            self.read_meta(location)
                .await
                .box_err()
                .map_err(|source| StoreError::NotFound {
                    path: location.to_string(),
                    source,
                })?;

        let covered_parts = meta
            .compute_covered_parts(range.clone())
            .box_err()
            .map_err(|source| StoreError::NotFound {
                path: location.to_string(),
                source,
            })?;

        if let Some(covered_parts) = covered_parts {
            let mut range_buffer = Vec::with_capacity(range.end - range.start);
            let keys: Vec<&[u8]> = covered_parts
                .part_keys
                .iter()
                .map(|key| key.as_bytes())
                .collect();
            let values =
                self.client
                    .get_batch(table_name, keys)
                    .map_err(|source| StoreError::NotFound {
                        path: location.to_string(),
                        source: Box::new(source),
                    })?;

            if covered_parts.part_keys.len() != values.len() {
                DataPartsLength {
                    part_len: covered_parts.part_keys.len(),
                    value_len: values.len(),
                }
                .fail()
                .map_err(|source| StoreError::Generic {
                    store: OBKV,
                    source: Box::new(source),
                })?
            }

            for (index, (key, value)) in covered_parts.part_keys.iter().zip(values).enumerate() {
                if let Some(bytes) = value {
                    let mut begin = 0;
                    let mut end = bytes.len() - 1;
                    if index == 0 {
                        begin = covered_parts.start_offset;
                    }
                    // the last one
                    if index == covered_parts.part_keys.len() - 1 {
                        end = covered_parts.end_offset;
                    }
                    range_buffer.extend_from_slice(&bytes[begin..=end]);
                } else {
                    DataPartNotFound { part_key: key }
                        .fail()
                        .map_err(|source| StoreError::NotFound {
                            path: location.to_string(),
                            source: Box::new(source),
                        })?;
                }
            }

            debug!("ObkvObjectStore get_range operation, location:{location}, table:{table_name}, cost:{:?}", instant.elapsed());

            return Ok(range_buffer.into());
        } else {
            return Ok(Bytes::new());
        }
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let instant = Instant::now();

        let meta =
            self.read_meta(location)
                .await
                .box_err()
                .map_err(|source| StoreError::NotFound {
                    path: location.to_string(),
                    source,
                })?;

        debug!(
            "ObkvObjectStore head operation, location:{location}, cost:{:?}",
            instant.elapsed()
        );

        let last_modified = self
            .convert_datetime(meta.last_modified)
            .box_err()
            .map_err(|source| StoreError::NotFound {
                path: location.to_string(),
                source,
            })?;

        Ok(ObjectMeta {
            location: (*location).clone(),
            last_modified,
            size: meta.size,
        })
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> Result<()> {
        let instant = Instant::now();

        // TODO: maybe coerruption here, should not delete data when data is reading.
        let table_name = self.pick_shard_table(location);
        let meta =
            self.read_meta(location)
                .await
                .box_err()
                .map_err(|source| StoreError::NotFound {
                    path: location.to_string(),
                    source,
                })?;

        // delete every part of data
        for part in &meta.parts {
            let key = part.as_bytes();
            self.client
                .delete(table_name, key)
                .map_err(|source| StoreError::Generic {
                    store: OBKV,
                    source: Box::new(source),
                })?;
        }
        // delete meta info
        self.meta_manager
            .delete(meta, location)
            .await
            .map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;

        debug!(
            "ObkvObjectStore delete operation, location:{location}, table:{table_name}, cost:{:?}",
            instant.elapsed()
        );

        Ok(())
    }

    /// List all the objects with the given prefix.
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a
    /// prefix of `foo/bar/x` but not of `foo/bar_baz/x`.
    /// TODO: Currently this method may return lots of object meta, we should
    /// limit the count of return ojects in future. Maybe a better
    /// implementation is to fetch and send the list results in a stream way.
    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let instant = Instant::now();

        let path = Self::normalize_path(prefix);
        let raw_metas =
            self.meta_manager
                .list(&path)
                .await
                .map_err(|source| StoreError::Generic {
                    store: OBKV,
                    source: Box::new(source),
                })?;

        let mut meta_list = Vec::new();
        for meta in raw_metas {
            meta_list.push(Ok(ObjectMeta {
                location: Path::from(meta.location),
                last_modified: Utc.timestamp_millis_opt(meta.last_modified).unwrap(),
                size: meta.size,
            }));
        }

        let iter = futures::stream::iter(meta_list.into_iter());
        debug!(
            "ObkvObjectStore list operation, prefix:{path}, cost:{:?}",
            instant.elapsed()
        );
        Ok(iter.boxed())
    }

    /// List all the objects and common paths(directories) with the given
    /// prefix. Prefixes are evaluated on a path segment basis, i.e.
    /// `foo/bar/` is a prefix of `foo/bar/x` but not of `foo/bar_baz/x`.
    /// see detail in: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let instant = Instant::now();

        let path = Self::normalize_path(prefix);
        let metas = self
            .meta_manager
            .list(&path)
            .await
            .map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;

        let mut common_prefixes = HashSet::new();
        let mut objects = Vec::new();
        for meta in metas {
            let location = meta.location;
            let subfix = &location[path.as_ref().len()..];
            if let Some(pos) = subfix.find(DELIMITER) {
                // common_prefix endswith '/'
                let common_prefix = &subfix[0..pos + 1];
                common_prefixes.insert(Path::from(common_prefix));
            } else {
                objects.push(ObjectMeta {
                    location: Path::from(location),
                    last_modified: Utc.timestamp_millis_opt(meta.last_modified).unwrap(),
                    size: meta.size,
                })
            }
        }

        let common_prefixes = Vec::from_iter(common_prefixes.into_iter());
        debug!(
            "ObkvObjectStore list_with_delimiter operation, prefix:{path}, cost:{:?}",
            instant.elapsed()
        );
        Ok(ListResult {
            common_prefixes,
            objects,
        })
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        // TODO:
        Err(StoreError::NotImplemented)
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        // TODO:
        Err(StoreError::NotImplemented)
    }
}

struct ObkvMultiPartUpload<T> {
    /// The full path to the object.
    location: Path,
    /// The id of multi upload tasks, we use this id as object version.
    upload_id: String,
    /// The table name of obkv to save data.
    table_name: String,
    /// The client of object store.
    client: Arc<T>,
    /// The size of object.
    size: AtomicU64,
    /// The size in bytes of one part. Note: maybe the size of last part less
    /// than part_size.
    part_size: usize,
    /// The mananger to process meta info.
    meta_manager: Arc<MetaManager<T>>,
}

struct PathKeyEncoder;

impl PathKeyEncoder {
    #[inline]
    fn part_key(location: &Path, upload_id: &str, part_idx: usize) -> String {
        format!("{location}@{upload_id}@{part_idx}")
    }

    #[inline]
    fn part_key_prefix(location: &Path, upload_id: &str) -> String {
        format!("{location}@{upload_id}@")
    }

    #[inline]
    fn unique_id(table: &str, location: &Path, upload_id: &str) -> String {
        format!("{table}@{location}@{upload_id}")
    }
}

#[async_trait]
impl<T: TableKv> CloudMultiPartUploadImpl for ObkvMultiPartUpload<T> {
    async fn put_multipart_part(
        &self,
        buf: Vec<u8>,
        part_idx: usize,
    ) -> Result<UploadPart, std::io::Error> {
        let mut batch = T::WriteBatch::default();
        let part_key = PathKeyEncoder::part_key(&self.location, &self.upload_id, part_idx);
        batch.insert(part_key.as_bytes(), buf.as_ref());

        self.client
            .write(WriteContext::default(), &self.table_name, batch)
            .map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;
        // Record size of object.
        self.size.fetch_add(buf.len() as u64, Ordering::Relaxed);
        Ok(UploadPart {
            content_id: part_key,
        })
    }

    async fn complete(&self, completed_parts: Vec<UploadPart>) -> Result<(), std::io::Error> {
        // We should save meta info after finish save data.
        let mut paths = Vec::with_capacity(completed_parts.len());
        for upload_part in completed_parts {
            paths.push(upload_part.content_id);
        }

        let now = SystemTime::now();
        let since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
        let last_modified = since_epoch.as_millis() as i64;

        let meta = ObkvObjectMeta {
            location: self.location.as_ref().to_string(),
            last_modified,
            size: self.size.load(Ordering::SeqCst) as usize,
            unique_id: Some(PathKeyEncoder::unique_id(
                &self.table_name,
                &self.location,
                &self.upload_id,
            )),
            part_size: self.part_size,
            parts: paths,
            version: self.upload_id.clone(),
        };

        // Save meta info to specify obkv table.
        // TODO: We should remove the previous object data when update object.
        self.meta_manager
            .save(meta)
            .await
            .map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;
        Ok(())
    }
}