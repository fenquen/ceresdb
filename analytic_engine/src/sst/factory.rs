// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Factory for different kinds sst writer and reader.

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use common_types::projected_schema::ProjectedSchema;
use macros::define_result;
use object_store::{ObjectStore, ObjectStoreRef, Path};
use runtime::Runtime;
use snafu::{ResultExt, Snafu};
use table_engine::predicate::Predicate;
use trace_metric::MetricsCollector;

use crate::{
    sst::{
        file::Level,
        header,
        header::HeaderParser,
        meta_data::cache::MetaCacheRef,
        parquet::{writer::ParquetSstWriter, AsyncParquetSstReader, ThreadedReader},
        reader::SstReader,
        writer::SstWriter,
    },
    table_options::{Compression, StorageFormat, StorageFormatHint},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to parse sst header, err:{}", source, ))]
    ParseHeader { source: header::Error },
}

define_result!(Error);

/// choose suitable object store for different scenes.
pub trait ObjectStoreChooser: Send + Sync + Debug {
    /// just provide default object store for the scenes where user don't care about it.
    fn chooseDefault(&self) -> &Arc<dyn ObjectStore>;

    /// choose an object store according to the read frequency.
    fn chooseByReadFrequency(&self, readFrequency: ReadFrequency) -> &Arc<dyn ObjectStore>;
}

pub type ObjectStorePickerRef = Arc<dyn ObjectStoreChooser>;

/// For any [`ObjectStoreRef`], it can be used as an [`ObjectStoreChooser`].
impl ObjectStoreChooser for Arc<dyn ObjectStore> {
    fn chooseDefault(&self) -> &ObjectStoreRef {
        self
    }

    fn chooseByReadFrequency(&self, _freq: ReadFrequency) -> &ObjectStoreRef {
        self
    }
}

pub type FactoryRef = Arc<dyn SstFactory>;

#[async_trait]
pub trait SstFactory: Send + Sync + Debug {
    async fn createReader<'a>(&self,
                              path: &'a Path,
                              options: &SstReadOptions,
                              hint: SstReadHint,
                              objectStoreChooser: &'a Arc<dyn ObjectStoreChooser>,
                              metrics_collector: Option<MetricsCollector>) -> Result<Box<dyn SstReader + Send + 'a>>;

    async fn createWriter<'a>(&self,
                              sstWriteOptions: &SstWriteOptions,
                              path: &'a Path,
                              objectStoreChooser: &'a Arc<dyn ObjectStoreChooser>,
                              level: Level) -> Result<Box<dyn SstWriter + Send + 'a>>;
}

/// The frequency of query execution may decide some behavior in the sst reader, e.g. cache policy.
#[derive(Debug, Copy, Clone)]
pub enum ReadFrequency {
    Once,
    Frequent,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SstReadHint {
    /// Hint for the size of the sst file. It may avoid some io if provided.
    pub file_size: Option<usize>,
    /// Hint for the storage format of the sst file. It may avoid some io if
    /// provided.
    pub file_format: Option<StorageFormat>,
}

#[derive(Debug, Clone)]
pub struct ScanOptions {
    /// The suggested parallelism while reading sst
    pub background_read_parallelism: usize,
    /// The max record batches in flight
    pub max_record_batches_in_flight: usize,
    /// The number of streams to prefetch when scan
    pub num_streams_to_prefetch: usize,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            background_read_parallelism: 1,
            max_record_batches_in_flight: 64,
            num_streams_to_prefetch: 2,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SstReadOptions {
    pub frequency: ReadFrequency,
    pub num_rows_per_row_group: usize,
    pub projected_schema: ProjectedSchema,
    pub predicate: Arc<Predicate>,
    pub meta_cache: Option<MetaCacheRef>,
    pub scan_options: ScanOptions,

    pub runtime: Arc<Runtime>,
}

#[derive(Debug, Clone)]
pub struct SstWriteOptions {
    pub storage_format_hint: StorageFormatHint,
    pub num_rows_per_row_group: usize,
    pub compression: Compression,
    pub max_buffer_size: usize,
}

#[derive(Debug, Default)]
pub struct SstFactoryImpl;

#[async_trait]
impl SstFactory for SstFactoryImpl {
    async fn createReader<'a>(&self,
                              path: &'a Path,
                              options: &SstReadOptions,
                              hint: SstReadHint,
                              objectStoreChooser: &'a Arc<dyn ObjectStoreChooser>,
                              metrics_collector: Option<MetricsCollector>) -> Result<Box<dyn SstReader + Send + 'a>> {
        let storage_format = match hint.file_format {
            Some(v) => v,
            None => {
                let header_parser = HeaderParser::new(path, objectStoreChooser.chooseDefault());
                header_parser.parse().await.context(ParseHeader)?
            }
        };

        match storage_format {
            StorageFormat::Columnar | StorageFormat::Hybrid => {
                let asyncParquetSstReader =
                    AsyncParquetSstReader::new(path,
                                               options,
                                               hint.file_size,
                                               objectStoreChooser,
                                               metrics_collector);

                let threadedReader =
                    ThreadedReader::new(asyncParquetSstReader,
                                        options.runtime.clone(),
                                        options.scan_options.background_read_parallelism,
                                        options.scan_options.max_record_batches_in_flight);

                Ok(Box::new(threadedReader))
            }
        }
    }

    async fn createWriter<'a>(&self,
                              sstWriteOptions: &SstWriteOptions,
                              sstFilePath: &'a Path,
                              objectStoreChooser: &'a Arc<dyn ObjectStoreChooser>,
                              level: Level) -> Result<Box<dyn SstWriter + Send + 'a>> {
        let useHybridEncoding = match sstWriteOptions.storage_format_hint {
            // `Auto` is mapped to columnar parquet format now, may change in future.
            StorageFormatHint::Auto => false,
            StorageFormatHint::Specific(format) => matches!(format, StorageFormat::Hybrid),
        };

        Ok(Box::new(ParquetSstWriter::new(
            sstFilePath,
            level,
            useHybridEncoding,
            objectStoreChooser,
            sstWriteOptions,
        )))
    }
}
