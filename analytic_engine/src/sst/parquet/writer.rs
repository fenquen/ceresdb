// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst writer implementation based on parquet.

use std::sync::Arc;
use async_trait::async_trait;
use common_types::{record_batch::RecordBatchWithKey, request_id::RequestId};
use datafusion::parquet::basic::Compression;
use futures::StreamExt;
use generic_error::BoxError;
use log::{debug, error};
use object_store::{ObjectStore, Path};
use snafu::ResultExt;
use tokio::io::AsyncWrite;

use super::meta_data::RowGroupFilter;
use crate::{
    sst::{
        factory::{ObjectStorePickerRef, SstWriteOptions},
        file::Level,
        parquet::{
            encoding::ParquetEncoder,
            meta_data::{ParquetFilter, ParquetMetaData, RowGroupFilterBuilder},
        },
        writer::{BuildParquetFilter, EncodeRecordBatch, SstMeta, PollRecordBatch,
                 RecordBatchStream, Result, SstInfo, SstWriter, Storage},
    },
    table_options::StorageFormat,
};

/// The implementation of sst based on parquet and object storage.
#[derive(Debug)]
pub struct ParquetSstWriter<'a> {
    sstFilePath: &'a Path,
    level: Level,
    useHybridEncoding: bool,
    objectStore: &'a Arc<dyn ObjectStore>,
    num_rows_per_row_group: usize,
    max_buffer_size: usize,
    compression: Compression,
}

impl<'a> ParquetSstWriter<'a> {
    pub fn new(sstFilePath: &'a Path,
               level: Level,
               useHybridEncoding: bool,
               objectStoreChooser: &'a ObjectStorePickerRef,
               sstWriteOptions: &SstWriteOptions) -> Self {
        let objectStore = objectStoreChooser.chooseDefault();
        Self {
            sstFilePath,
            level,
            useHybridEncoding,
            objectStore,
            num_rows_per_row_group: sstWriteOptions.num_rows_per_row_group,
            compression: sstWriteOptions.compression.into(),
            max_buffer_size: sstWriteOptions.max_buffer_size,
        }
    }
}

/// The writer will reorganize the record batches into row groups, and then encode them to parquet file.
struct RecordBatchGroupWriter {
    requestId: RequestId,
    useHybridEncoding: bool,
    /// stream化的memTable
    input: RecordBatchStream,
    inputExhausted: bool,
    sstMeta: SstMeta,
    rowCountPerRowGroup: usize,
    max_buffer_size: usize,
    compression: Compression,
    level: Level,
}

impl RecordBatchGroupWriter {
    /// Fetch an integral row group from the `self.input`.
    ///
    /// Except the last one, every row group is ensured to contains exactly
    /// `self.num_rows_per_row_group`. As for the last one, it will cover all the left rows.
    async fn fetchNextRowGroup(&mut self, prevRecordBatchWithKey: &mut Option<RecordBatchWithKey>) -> Result<Vec<RecordBatchWithKey>> {
        let mut recordBatchWithKeyVec = vec![];

        // rowGroup包含了多个recordBatchWithKey 而 recordBatchWithKey 包含多个 row的 fenquen
        // used to record the number of remaining rows to fill `curr_row_group`.
        let mut remaining = self.rowCountPerRowGroup;

        // keep filling `curr_row_group` until `remaining` is zero.
        while remaining > 0 {
            // use the `prev_record_batch` to fill `curr_row_group` if possible.
            if let Some(v) = prevRecordBatchWithKey {
                let total_rows = v.rowCount();

                if total_rows <= remaining {
                    // the whole record batch is part of the `curr_row_group`, and let's feed it into `curr_row_group`.
                    recordBatchWithKeyVec.push(prevRecordBatchWithKey.take().unwrap());
                    remaining -= total_rows;
                } else {
                    // only first `remaining` rows of the record batch belongs to `curr_row_group`,
                    // the rest should be put to `prev_record_batch` for next row group.
                    recordBatchWithKeyVec.push(v.slice(0, remaining));
                    *v = v.slice(remaining, total_rows - remaining);
                    remaining = 0;
                }

                continue;
            }

            if self.inputExhausted {
                break;
            }

            // 之前的recordBatch已经用光了,拿取下轮
            match self.input.next().await {
                Some(v) => {
                    let recordBatchWithKey = v.context(PollRecordBatch)?;
                    debug_assert!(!recordBatchWithKey.is_empty(), "found empty record batch, request id:{}", self.requestId);

                    // updated the exhausted `prev_record_batch`, and let next loop to continue to fill `curr_row_group`.
                    prevRecordBatchWithKey.replace(recordBatchWithKey);
                }
                None => {
                    self.inputExhausted = true;
                    break;
                }
            };
        }

        Ok(recordBatchWithKeyVec)
    }

    /// Build the parquet filter for the given `row_group`.
    fn build_row_group_filter(&self, row_group_batch: &[RecordBatchWithKey]) -> Result<RowGroupFilter> {
        let mut builder = RowGroupFilterBuilder::new(row_group_batch[0].schema_with_key());

        for partial_batch in row_group_batch {
            for (col_idx, column) in partial_batch.columns().iter().enumerate() {
                for row in 0..column.num_rows() {
                    let datum_view = column.datum_view(row);
                    datum_view.do_with_bytes(|bytes| {
                        builder.add_key(col_idx, bytes);
                    });
                }
            }
        }

        builder.build().box_err().context(BuildParquetFilter)
    }

    fn needCustomFilter(&self) -> bool {
        // TODO: support filter in hybrid storage format [#435](https://github.com/CeresDB/ceresdb/issues/435)
        !self.useHybridEncoding && !self.level.is_min()
    }

    async fn writeAll<W: AsyncWrite + Send + Unpin + 'static>(mut self, writer: W) -> Result<usize> {
        // iter运行1把的成果便是RecordBatchWithKey
        let mut prevRecordBatchWithKey: Option<RecordBatchWithKey> = None;
        let mut arrowRecordBatchVec = Vec::new();
        let mut totalRowNum = 0;

        let mut parquetEncoder =
            ParquetEncoder::new(writer,
                                &self.sstMeta.schema,
                                self.useHybridEncoding,
                                self.rowCountPerRowGroup,
                                self.max_buffer_size,
                                self.compression).box_err().context(EncodeRecordBatch)?;

        let mut parquet_filter = if self.needCustomFilter() {
            Some(ParquetFilter::default())
        } else {
            None
        };

        loop {
            // rowGroup 是 RecordBatchWithKey 的Vec fenquen
            let recordBatchWithKeyVec = self.fetchNextRowGroup(&mut prevRecordBatchWithKey).await?;

            if recordBatchWithKeyVec.is_empty() {
                break;
            }

            if let Some(filter) = &mut parquet_filter {
                filter.row_group_filters.push(self.build_row_group_filter(&recordBatchWithKeyVec)?);
            }

            let batchNum = recordBatchWithKeyVec.len();
            for recordBatchWithKey in recordBatchWithKeyVec {
                arrowRecordBatchVec.push(recordBatchWithKey.recordBatchData.arrowRecordBatch);
            }

            let rowNum = parquetEncoder.recordEncoder.encode(arrowRecordBatchVec).await.box_err().context(EncodeRecordBatch)?;

            // TODO: it will be better to use `arrow_row_group.clear()` to reuse the allocated memory.
            arrowRecordBatchVec = Vec::with_capacity(batchNum);

            totalRowNum += rowNum;
        }

        let parquetMetaData = {
            let mut parquetMetaData = ParquetMetaData::from(self.sstMeta);
            parquetMetaData.parquet_filter = parquet_filter;
            parquetMetaData
        };

        parquetEncoder.recordEncoder.encodeParquetMetaData(parquetMetaData).box_err().context(EncodeRecordBatch)?;

        parquetEncoder.recordEncoder.close().await.box_err().context(EncodeRecordBatch)?;

        Ok(totalRowNum)
    }
}

struct ObjectStoreMultiUploadAbortor<'a> {
    location: &'a Path,
    sessionId: String,
    objectStore: &'a Arc<dyn ObjectStore>,
}

impl<'a> ObjectStoreMultiUploadAbortor<'a> {
    async fn initialize(objectStore: &'a Arc<dyn ObjectStore>,
                        location: &'a Path) -> Result<(ObjectStoreMultiUploadAbortor<'a>, Box<dyn AsyncWrite + Unpin + Send>, )> {
        let (sessionId, writer) = objectStore.put_multipart(location).await.context(Storage)?;

        let aborter = Self {
            location,
            sessionId,
            objectStore,
        };

        Ok((aborter, writer))
    }

    async fn abort(self) -> Result<()> {
        self.objectStore.abort_multipart(self.location, &self.sessionId).await.context(Storage)
    }
}

#[async_trait]
impl<'a> SstWriter for ParquetSstWriter<'a> {
    async fn write(&mut self,
                   requestId: RequestId,
                   sstMeta: &SstMeta,
                   input: RecordBatchStream) -> Result<SstInfo> {
        debug!("build parquet file, request_id:{}, meta:{:?}, num_rows_per_row_group:{}",requestId, sstMeta, self.num_rows_per_row_group);

        let recordBatchGroupWriter = RecordBatchGroupWriter {
            useHybridEncoding: self.useHybridEncoding,
            requestId,
            input,
            inputExhausted: false,
            rowCountPerRowGroup: self.num_rows_per_row_group,
            max_buffer_size: self.max_buffer_size,
            compression: self.compression,
            sstMeta: sstMeta.clone(),
            level: self.level,
        };

        let (aborter, writer) =
            ObjectStoreMultiUploadAbortor::initialize(self.objectStore, self.sstFilePath).await?;

        let totalRowNum = match recordBatchGroupWriter.writeAll(writer).await {
            Ok(v) => v,
            Err(e) => {
                if let Err(e) = aborter.abort().await {
                    // the uploading file will be leaked if failed to abort. A repair command will be provided to clean up the leaked files.
                    error!("failed to abort multi-upload for sst:{}, err:{}", self.sstFilePath, e);
                }
                return Err(e);
            }
        };

        let file_head = self.objectStore.head(self.sstFilePath).await.context(Storage)?;

        let storage_format = if self.useHybridEncoding {
            StorageFormat::Hybrid
        } else {
            StorageFormat::Columnar
        };

        Ok(SstInfo {
            file_size: file_head.size,
            row_num: totalRowNum,
            storage_format,
        })
    }
}