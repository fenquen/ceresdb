// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    ops::{Bound, Not},
    sync::Arc,
    time::Instant,
};

use arrow::{
    array::BooleanArray,
    datatypes::{DataType as ArrowDataType, SchemaRef as ArrowSchemaRef},
};
use common_types::{
    projected_schema::ProjectedSchema, record_batch::RecordBatchWithKey, SequenceNumber,
};
use datafusion::{
    common::ToDFSchema,
    error::DataFusionError,
    optimizer::utils as dataFusionOptUtils,
    physical_expr::{self, execution_props::ExecutionProps},
    physical_plan::PhysicalExpr,
};
use futures::stream::{self, StreamExt};
use generic_error::{BoxError, GenericResult};
use macros::define_result;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::{predicate::Predicate, table::TableId};
use trace_metric::MetricsCollector;
use crate::memtable;

use crate::{
    memtable::{MemTableRef, ScanRequest},
    prefetchable_stream::{NoopPrefetcher, PrefetchableStream, PrefetchableStreamExt},
    space::SpaceId,
    sst::{
        factory::{
            self, FactoryRef as SstFactoryRef, ObjectStorePickerRef, SstReadHint, SstReadOptions,
        },
        file::FileHandle,
    },
    table::sst_util,
};
use crate::sst::factory::{ObjectStoreChooser, SstFactory};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to create sst reader, err:{:?}", source, ))]
    CreateSstReader { source: factory::Error },

    #[snafu(display("Fail to read sst meta, err:{}", source))]
    ReadSstMeta { source: crate::sst::reader::Error },

    #[snafu(display("Fail to read sst data, err:{}", source))]
    ReadSstData { source: crate::sst::reader::Error },

    #[snafu(display("Fail to scan memtable, err:{}", source))]
    ScanMemtable { source: crate::memtable::Error },

    #[snafu(display("fail to execute filter expression, err:{}.\nBacktrace:\n{}", source, backtrace))]
    FilterExec {
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("fail to downcast boolean array, actual data type:{:?}.\nBacktrace:\n{}", data_type, backtrace))]
    DowncastBooleanArray {
        data_type: ArrowDataType,
        backtrace: Backtrace,
    },

    #[snafu(display("failed to get datafusion schema, err:{}.\nBacktrace:\n{}", source, backtrace
    ))]
    DatafusionSchema {
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("failed to generate datafusion physical expr, err:{}.\nBacktrace:\n{}", source, backtrace))]
    DatafusionExpr {
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to select from record batch, err:{}", source))]
    SelectBatchData {
        source: common_types::record_batch::Error,
    },

    #[snafu(display("timeout when read record batch, err:{}.\nBacktrace:\n{}", source, backtrace))]
    Timeout {
        source: tokio::time::error::Elapsed,
        backtrace: Backtrace,
    },
}

define_result!(Error);

// TODO(yingwen): Can we move sequence to RecordBatchWithKey and remove this struct? But what is the sequence after merge?
#[derive(Debug)]
pub struct SequencedRecordBatch {
    pub recordBatchWithKey: RecordBatchWithKey,
    pub sequence: SequenceNumber,
}

impl SequencedRecordBatch {
    #[inline]
    pub fn num_rows(&self) -> usize {
        self.recordBatchWithKey.rowCount()
    }
}

pub type BoxedPrefetchableRecordBatchStream = Box<dyn PrefetchableStream<Item=GenericResult<SequencedRecordBatch>>>;

/// Filter the sequenced record batch stream by applying the `predicate`.
pub fn filterStream(stream: Box<dyn PrefetchableStream<Item=GenericResult<SequencedRecordBatch>>>,
                    arrowSchema: ArrowSchemaRef,
                    predicate: &Predicate) -> Result<BoxedPrefetchableRecordBatchStream> {
    let filter = match dataFusionOptUtils::conjunction(predicate.exprs().to_owned()) {
        Some(filter) => filter,
        None => return Ok(stream),
    };

    let input_df_schema = arrowSchema.clone().to_dfschema().context(DatafusionSchema)?;

    let predicate =
        physical_expr::create_physical_expr(&filter,
                                            &input_df_schema,
                                            arrowSchema.as_ref(),
                                            &ExecutionProps::new()).context(DatafusionExpr)?;

    let filteredStream =
        stream.filter_map(move |sequence_record_batch| match sequence_record_batch {
            Ok(sequencedRecordBatch) => filterRecordBatch(sequencedRecordBatch, predicate.clone()).box_err().transpose(),
            Err(e) => Some(Err(e)),
        });

    Ok(Box::new(filteredStream))
}

/// Filter the `sequenced_record_batch` according to the `predicate`.
fn filterRecordBatch(mut sequencedRecordBatch: SequencedRecordBatch, predicate: Arc<dyn PhysicalExpr>) -> Result<Option<SequencedRecordBatch>> {
    let arrowRecordBatch = &sequencedRecordBatch.recordBatchWithKey.recordBatchData.arrowRecordBatch;

    let filterArray =
        predicate.evaluate(arrowRecordBatch).map(|v| v.into_array(arrowRecordBatch.num_rows())).context(FilterExec)?;

    let selectedRows =
        filterArray.as_any().downcast_ref::<BooleanArray>().context(DowncastBooleanArray { data_type: filterArray.as_ref().data_type().clone() })?;

    sequencedRecordBatch.recordBatchWithKey.selectData(selectedRows).context(SelectBatchData)?;

    sequencedRecordBatch.recordBatchWithKey.is_empty().not().then_some(Ok(sequencedRecordBatch)).transpose()
}

/// 用来select时候读取memTable Build [SequencedRecordBatchStream] from a memtable.
/// build filtered (by `predicate`) [SequencedRecordBatchStream] from a memtable.
pub fn filteredStreamFromMemTable(projectedSchema: ProjectedSchema,
                                  need_dedup: bool,
                                  memtable: &MemTableRef,
                                  reverse: bool,
                                  predicate: &Predicate,
                                  deadline: Option<Instant>,
                                  metrics_collector: Option<MetricsCollector>) -> Result<BoxedPrefetchableRecordBatchStream> {
    let max_seq = memtable.last_sequence();

    let scanRequest = ScanRequest {
        start_user_key: Bound::Unbounded,
        end_user_key: Bound::Unbounded,
        maxVisibleSeq: max_seq,
        projected_schema: projectedSchema.clone(),
        need_dedup,
        reverse,
        metrics_collector: metrics_collector.map(|v| v.span(format!("scan_memtable_{}", max_seq))),
        batchSize: memtable::DEFAULT_SCAN_BATCH_SIZE,
        deadline,
    };

    let iter = memtable.scan(scanRequest).context(ScanMemtable)?;

    let stream = stream::iter(iter).map(move |v| {
        v.map(|recordBatchWithKey| SequencedRecordBatch { recordBatchWithKey, sequence: max_seq }).box_err()
    });

    filterStream(Box::new(NoopPrefetcher(Box::new(stream))),
                 projectedSchema.0.recordSchemaWithKey.recordSchema.arrowSchema.clone(),
                 predicate)
}

/// Build the [SequencedRecordBatchStream] from a sst.
/// Build the filtered by `sst_read_options.predicate` [SequencedRecordBatchStream] from a sst.
pub async fn filteredStreamFromSst(space_id: SpaceId,
                                   table_id: TableId,
                                   fileHandle: &FileHandle,
                                   sst_factory: &Arc<dyn SstFactory>,
                                   sst_read_options: &SstReadOptions,
                                   store_picker: &Arc<dyn ObjectStoreChooser>,
                                   metrics_collector: Option<MetricsCollector>) -> Result<BoxedPrefetchableRecordBatchStream> {
    fileHandle.read_meter().mark();

    let sstFilePath = sst_util::buildSstFilePath(space_id, table_id, fileHandle.fileHandleInner.meta.id);

    let read_hint = SstReadHint {
        file_size: Some(fileHandle.fileHandleInner.meta.size as usize),
        file_format: Some(fileHandle.fileHandleInner.meta.storage_format),
    };

    let metrics_collector = metrics_collector.map(|v| v.span(format!("scan_sst_{}", fileHandle.id())));

    let mut sst_reader =
        sst_factory.createReader(&sstFilePath,
                                 sst_read_options,
                                 read_hint,
                                 store_picker,
                                 metrics_collector).await.context(CreateSstReader)?;
    let meta = sst_reader.meta_data().await.context(ReadSstMeta)?;
    let max_seq = meta.max_sequence();
    let stream = sst_reader.read().await.context(ReadSstData)?;
    let stream = stream.map(move |v| {
        v.map(|record_batch| SequencedRecordBatch {
            recordBatchWithKey: record_batch,
            sequence: max_seq,
        }).box_err()
    });

    filterStream(Box::new(stream),
                 sst_read_options.projected_schema.as_record_schema_with_key().to_arrow_schema_ref(),
                 sst_read_options.predicate.as_ref())
}

pub async fn stream_from_sst_file(space_id: SpaceId,
                                  table_id: TableId,
                                  sst_file: &FileHandle,
                                  sst_factory: &SstFactoryRef,
                                  sst_read_options: &SstReadOptions,
                                  store_picker: &ObjectStorePickerRef,
                                  metrics_collector: Option<MetricsCollector>) -> Result<BoxedPrefetchableRecordBatchStream> {
    sst_file.read_meter().mark();

    let path = sst_util::buildSstFilePath(space_id, table_id, sst_file.id());

    let read_hint = SstReadHint {
        file_size: Some(sst_file.size() as usize),
        file_format: Some(sst_file.storage_format()),
    };
    let scan_sst_desc = format!("scan_sst_{}", sst_file.id());
    let metrics_collector = metrics_collector.map(|v| v.span(scan_sst_desc));
    let mut sst_reader =
        sst_factory.createReader(&path,
                                 sst_read_options,
                                 read_hint,
                                 store_picker,
                                 metrics_collector).await.context(CreateSstReader)?;
    let meta = sst_reader.meta_data().await.context(ReadSstMeta)?;
    let max_seq = meta.max_sequence();
    let stream = sst_reader.read().await.context(ReadSstData)?;
    let stream = stream.map(move |v| {
        v.map(|record_batch| SequencedRecordBatch {
            recordBatchWithKey: record_batch,
            sequence: max_seq,
        })
            .box_err()
    });

    Ok(Box::new(stream))
}