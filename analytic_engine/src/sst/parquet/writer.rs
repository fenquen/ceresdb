// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst writer implementation based on parquet.

use async_trait::async_trait;
use common_types::{record_batch::RecordBatchWithKey, request_id::RequestId};
use datafusion::parquet::basic::Compression;
use futures::StreamExt;
use generic_error::BoxError;
use log::{debug, error};
use object_store::{ObjectStoreRef, Path};
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
        writer::{
            self, BuildParquetFilter, EncodeRecordBatch, MetaData, PollRecordBatch,
            RecordBatchStream, Result, SstInfo, SstWriter, Storage,
        },
    },
    table_options::StorageFormat,
};

/// The implementation of sst based on parquet and object storage.
#[derive(Debug)]
pub struct ParquetSstWriter<'a> {
    /// The path where the data is persisted.
    path: &'a Path,
    level: Level,
    hybrid_encoding: bool,
    /// The storage where the data is persist.
    store: &'a ObjectStoreRef,
    /// Max row group size.
    num_rows_per_row_group: usize,
    max_buffer_size: usize,
    compression: Compression,
}

impl<'a> ParquetSstWriter<'a> {
    pub fn new(
        path: &'a Path,
        level: Level,
        hybrid_encoding: bool,
        store_picker: &'a ObjectStorePickerRef,
        options: &SstWriteOptions,
    ) -> Self {
        let store = store_picker.default_store();
        Self {
            path,
            level,
            hybrid_encoding,
            store,
            num_rows_per_row_group: options.num_rows_per_row_group,
            compression: options.compression.into(),
            max_buffer_size: options.max_buffer_size,
        }
    }
}

/// The writer will reorganize the record batches into row groups, and then
/// encode them to parquet file.
struct RecordBatchGroupWriter {
    request_id: RequestId,
    hybrid_encoding: bool,
    input: RecordBatchStream,
    input_exhausted: bool,
    meta_data: MetaData,
    num_rows_per_row_group: usize,
    max_buffer_size: usize,
    compression: Compression,
    level: Level,
}

impl RecordBatchGroupWriter {
    /// Fetch an integral row group from the `self.input`.
    ///
    /// Except the last one, every row group is ensured to contains exactly
    /// `self.num_rows_per_row_group`. As for the last one, it will cover all
    /// the left rows.
    async fn fetch_next_row_group(
        &mut self,
        prev_record_batch: &mut Option<RecordBatchWithKey>,
    ) -> Result<Vec<RecordBatchWithKey>> {
        let mut curr_row_group = vec![];
        // Used to record the number of remaining rows to fill `curr_row_group`.
        let mut remaining = self.num_rows_per_row_group;

        // Keep filling `curr_row_group` until `remaining` is zero.
        while remaining > 0 {
            // Use the `prev_record_batch` to fill `curr_row_group` if possible.
            if let Some(v) = prev_record_batch {
                let total_rows = v.num_rows();
                if total_rows <= remaining {
                    // The whole record batch is part of the `curr_row_group`, and let's feed it
                    // into `curr_row_group`.
                    curr_row_group.push(prev_record_batch.take().unwrap());
                    remaining -= total_rows;
                } else {
                    // Only first `remaining` rows of the record batch belongs to `curr_row_group`,
                    // the rest should be put to `prev_record_batch` for next row group.
                    curr_row_group.push(v.slice(0, remaining));
                    *v = v.slice(remaining, total_rows - remaining);
                    remaining = 0;
                }

                continue;
            }

            if self.input_exhausted {
                break;
            }

            // Previous record batch has been exhausted, and let's fetch next record batch.
            match self.input.next().await {
                Some(v) => {
                    let v = v.context(PollRecordBatch)?;
                    debug_assert!(
                        !v.is_empty(),
                        "found empty record batch, request id:{}",
                        self.request_id
                    );

                    // Updated the exhausted `prev_record_batch`, and let next loop to continue to
                    // fill `curr_row_group`.
                    prev_record_batch.replace(v);
                }
                None => {
                    self.input_exhausted = true;
                    break;
                }
            };
        }

        Ok(curr_row_group)
    }

    /// Build the parquet filter for the given `row_group`.
    fn build_row_group_filter(
        &self,
        row_group_batch: &[RecordBatchWithKey],
    ) -> Result<RowGroupFilter> {
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

    fn need_custom_filter(&self) -> bool {
        // TODO: support filter in hybrid storage format [#435](https://github.com/CeresDB/ceresdb/issues/435)
        !self.hybrid_encoding && !self.level.is_min()
    }

    async fn write_all<W: AsyncWrite + Send + Unpin + 'static>(mut self, sink: W) -> Result<usize> {
        let mut prev_record_batch: Option<RecordBatchWithKey> = None;
        let mut arrow_row_group = Vec::new();
        let mut total_num_rows = 0;

        let mut parquet_encoder = ParquetEncoder::try_new(
            sink,
            &self.meta_data.schema,
            self.hybrid_encoding,
            self.num_rows_per_row_group,
            self.max_buffer_size,
            self.compression,
        )
        .box_err()
        .context(EncodeRecordBatch)?;
        let mut parquet_filter = if self.need_custom_filter() {
            Some(ParquetFilter::default())
        } else {
            None
        };

        loop {
            let row_group = self.fetch_next_row_group(&mut prev_record_batch).await?;
            if row_group.is_empty() {
                break;
            }

            if let Some(filter) = &mut parquet_filter {
                filter.push_row_group_filter(self.build_row_group_filter(&row_group)?);
            }

            let num_batches = row_group.len();
            for record_batch in row_group {
                arrow_row_group.push(record_batch.into_record_batch().into_arrow_record_batch());
            }
            let num_rows = parquet_encoder
                .encode_record_batches(arrow_row_group)
                .await
                .box_err()
                .context(EncodeRecordBatch)?;

            // TODO: it will be better to use `arrow_row_group.clear()` to reuse the
            // allocated memory.
            arrow_row_group = Vec::with_capacity(num_batches);
            total_num_rows += num_rows;
        }

        let parquet_meta_data = {
            let mut parquet_meta_data = ParquetMetaData::from(self.meta_data);
            parquet_meta_data.parquet_filter = parquet_filter;
            parquet_meta_data
        };
        parquet_encoder
            .set_meta_data(parquet_meta_data)
            .box_err()
            .context(EncodeRecordBatch)?;

        parquet_encoder
            .close()
            .await
            .box_err()
            .context(EncodeRecordBatch)?;

        Ok(total_num_rows)
    }
}

struct ObjectStoreMultiUploadAborter<'a> {
    location: &'a Path,
    session_id: String,
    object_store: &'a ObjectStoreRef,
}

impl<'a> ObjectStoreMultiUploadAborter<'a> {
    async fn initialize_upload(
        object_store: &'a ObjectStoreRef,
        location: &'a Path,
    ) -> Result<(
        ObjectStoreMultiUploadAborter<'a>,
        Box<dyn AsyncWrite + Unpin + Send>,
    )> {
        let (session_id, upload_writer) = object_store
            .put_multipart(location)
            .await
            .context(Storage)?;
        let aborter = Self {
            location,
            session_id,
            object_store,
        };
        Ok((aborter, upload_writer))
    }

    async fn abort(self) -> Result<()> {
        self.object_store
            .abort_multipart(self.location, &self.session_id)
            .await
            .context(Storage)
    }
}

#[async_trait]
impl<'a> SstWriter for ParquetSstWriter<'a> {
    async fn write(
        &mut self,
        request_id: RequestId,
        meta: &MetaData,
        input: RecordBatchStream,
    ) -> writer::Result<SstInfo> {
        debug!(
            "Build parquet file, request_id:{}, meta:{:?}, num_rows_per_row_group:{}",
            request_id, meta, self.num_rows_per_row_group
        );

        let group_writer = RecordBatchGroupWriter {
            hybrid_encoding: self.hybrid_encoding,
            request_id,
            input,
            input_exhausted: false,
            num_rows_per_row_group: self.num_rows_per_row_group,
            max_buffer_size: self.max_buffer_size,
            compression: self.compression,
            meta_data: meta.clone(),
            level: self.level,
        };

        let (aborter, sink) =
            ObjectStoreMultiUploadAborter::initialize_upload(self.store, self.path).await?;
        let total_num_rows = match group_writer.write_all(sink).await {
            Ok(v) => v,
            Err(e) => {
                if let Err(e) = aborter.abort().await {
                    // The uploading file will be leaked if failed to abort. A repair command will
                    // be provided to clean up the leaked files.
                    error!(
                        "Failed to abort multi-upload for sst:{}, err:{}",
                        self.path, e
                    );
                }
                return Err(e);
            }
        };

        let file_head = self.store.head(self.path).await.context(Storage)?;
        let storage_format = if self.hybrid_encoding {
            StorageFormat::Hybrid
        } else {
            StorageFormat::Columnar
        };
        Ok(SstInfo {
            file_size: file_head.size,
            row_num: total_num_rows,
            storage_format,
        })
    }
}