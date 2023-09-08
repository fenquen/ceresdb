// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst reader implementation based on parquet.

use std::{
    ops::Range,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch as ArrowRecordBatch};
use async_trait::async_trait;
use bytes_ext::Bytes;
use common_types::{
    projected_schema::{ProjectedSchema, RowProjector},
    record_batch::{ArrowRecordBatchProjector, RecordBatchWithKey},
};
use datafusion::{common::ToDFSchema, datasource::physical_plan::{parquet::page_filter::PagePruningPredicate, ParquetFileMetrics}, physical_expr::{execution_props::ExecutionProps}, physical_expr, physical_plan::metrics::ExecutionPlanMetricsSet};
use futures::{Stream, StreamExt};
use generic_error::{BoxError, GenericResult};
use log::{debug, error};
use object_store::{ObjectStore, ObjectStoreRef, Path};
use parquet::{
    arrow::{arrow_reader::RowSelection, ParquetRecordBatchStreamBuilder, ProjectionMask},
    file::metadata::RowGroupMetaData,
};
use parquet_ext::{meta_data::ChunkReader, reader::ObjectStoreReader};
use runtime::{AbortOnDropMany, JoinHandle, Runtime};
use snafu::ResultExt;
use table_engine::predicate::Predicate;
use time_ext::InstantExt;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    watch,
};
use trace_metric::{MetricsCollector, TraceMetricWhenDrop};

use crate::{
    prefetchable_stream::{NoopPrefetcher, PrefetchableStream},
    sst::{
        factory::{ObjectStorePickerRef, ReadFrequency, SstReadOptions},
        meta_data::{
            cache:: MetaData,
            SstMetaData,
        },
        parquet::{
            encoding::ParquetDecoder,
            meta_data::{ParquetFilter, ParquetMetaDataRef},
            row_group_pruner::RowGroupPruner,
        },
        reader::{error::*, Result, SstReader},
    },
};
use crate::sst::meta_data::cache::MetaCache;
use crate::sst::parquet::encoding::{ArrowRecordBatchDecoder, ColumnarRecordDecoder, HybridRecordDecoder};
use crate::sst::parquet::meta_data::ParquetMetaData;

const PRUNE_ROW_GROUPS_METRICS_COLLECTOR_NAME: &str = "prune_row_groups";

type ArrowRecordBatchStream = Pin<Box<dyn Stream<Item=Result<ArrowRecordBatch>> + Send>>;
type RecordBatchWithKeyStream = Box<dyn Stream<Item=Result<RecordBatchWithKey>> + Send + Unpin>;

#[derive(Default, Debug, Clone, TraceMetricWhenDrop)]
pub(crate) struct Metrics {
    #[metric(boolean)]
    pub meta_data_cache_hit: bool,
    #[metric(duration)]
    pub read_meta_data_duration: Duration,
    #[metric(number)]
    pub parallelism: usize,
    #[metric(collector)]
    pub metricsCollector: Option<MetricsCollector>,
}

impl<'a> Drop for AsyncParquetSstReader<'a> {
    fn drop(&mut self) {
        debug!("parquet reader dropped, path:{:?}, df_plan_metrics:{}",self.path,self.df_plan_metrics.clone_inner().to_string());
    }
}

pub struct ChunkReaderAdapter<'a> {
    path: &'a Path,
    store: &'a ObjectStoreRef,
}

impl<'a> ChunkReaderAdapter<'a> {
    pub fn new(path: &'a Path, store: &'a ObjectStoreRef) -> Self {
        Self { path, store }
    }
}

#[async_trait]
impl<'a> ChunkReader for ChunkReaderAdapter<'a> {
    async fn get_bytes(&self, range: Range<usize>) -> GenericResult<Bytes> {
        self.store.get_range(self.path, range).await.box_err()
    }
}

#[derive(Default, Debug, Clone, TraceMetricWhenDrop)]
pub(crate) struct ProjectorMetrics {
    #[metric(number, sum)]
    pub row_num: usize,
    #[metric(number, sum)]
    pub row_mem: usize,
    #[metric(duration, sum)]
    pub project_record_batch: Duration,
    #[metric(collector)]
    pub metrics_collector: Option<MetricsCollector>,
}

struct RecordBatchProjector {
    stream: ArrowRecordBatchStream,
    arrowRecordBatchProjector: ArrowRecordBatchProjector,

    metrics: ProjectorMetrics,
    start_time: Instant,
    sst_meta: Arc<ParquetMetaData>,
}

impl RecordBatchProjector {
    fn new(stream: ArrowRecordBatchStream,
           arrowRecordBatchProjector: ArrowRecordBatchProjector,
           sst_meta: ParquetMetaDataRef,
           metrics_collector: Option<MetricsCollector>) -> RecordBatchProjector {
        let metrics = ProjectorMetrics {
            metrics_collector,
            ..Default::default()
        };

        RecordBatchProjector {
            stream,
            arrowRecordBatchProjector,
            metrics,
            start_time: Instant::now(),
            sst_meta,
        }
    }
}

/// arrowRecordBatch 变成 recordBatch
impl Stream for RecordBatchProjector {
    type Item = Result<RecordBatchWithKey>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projector = self.get_mut();

        match projector.stream.poll_next_unpin(cx) {
            // 得到原始的 arrowRecordBatch
            Poll::Ready(Some(arrowRecordBatch)) => {
                match arrowRecordBatch.box_err().context(DecodeRecordBatch {}) {
                    Ok(arrowRecordBatch) => {
                        let arrowRecordBatchDecoder: Box<dyn ArrowRecordBatchDecoder> = if projector.sst_meta.collapsible_cols_idx.is_empty() {
                            Box::new(ColumnarRecordDecoder {})
                        } else {
                            Box::new(HybridRecordDecoder { collapsible_cols_idx: projector.sst_meta.collapsible_cols_idx.to_vec() })
                        };
                        // ParquetDecoder::new(&projector.sst_meta.collapsible_cols_idx);

                        let arrowRecordBatch = arrowRecordBatchDecoder.decode(arrowRecordBatch).box_err().context(DecodeRecordBatch)?;

                        for col in arrowRecordBatch.columns() {
                            projector.metrics.row_mem += col.get_array_memory_size();
                        }
                        projector.metrics.row_num += arrowRecordBatch.num_rows();

                        let recordBatchWithKey =
                            projector.arrowRecordBatchProjector.project2RecordBatchWithKey(arrowRecordBatch).box_err().context(DecodeRecordBatch {});

                        Poll::Ready(Some(recordBatchWithKey))
                    }
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                projector.metrics.project_record_batch += projector.start_time.saturating_elapsed();
                Poll::Ready(None)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

pub struct AsyncParquetSstReader<'a> {
    /// The path where the data is persisted.
    path: &'a Path,
    /// The storage where the data is persist.
    objectStore: &'a Arc<dyn ObjectStore>,
    /// The hint for the sst file size.
    file_size_hint: Option<usize>,
    num_rows_per_row_group: usize,
    projected_schema: ProjectedSchema,
    meta_cache: Option<Arc<MetaCache>>,
    predicate: Arc<Predicate>,
    /// Current frequency decides the cache policy.
    frequency: ReadFrequency,
    /// Init those fields in `init_if_necessary`
    meta_data: Option<MetaData>,
    row_projector: Option<RowProjector>,

    /// Options for `read_parallelly`
    metrics: Metrics,
    df_plan_metrics: ExecutionPlanMetricsSet,
}

#[async_trait]
impl<'a> SstReader for AsyncParquetSstReader<'a> {
    async fn meta_data(&mut self) -> Result<SstMetaData> {
        self.init().await?;

        Ok(SstMetaData::Parquet(self.meta_data.as_ref().unwrap().custom().clone()))
    }

    async fn read(&mut self) -> Result<Box<dyn PrefetchableStream<Item=Result<RecordBatchWithKey>>>> {
        let mut streams = self.parallelRead(1).await?;
        assert_eq!(streams.len(), 1);
        let stream = streams.pop().expect("impossible to fetch no stream");

        Ok(Box::new(NoopPrefetcher(stream)))
    }
}

impl<'a> AsyncParquetSstReader<'a> {
    pub fn new(path: &'a Path,
               options: &SstReadOptions,
               file_size_hint: Option<usize>,
               store_picker: &'a ObjectStorePickerRef,
               metrics_collector: Option<MetricsCollector>) -> AsyncParquetSstReader<'a> {
        let store = store_picker.chooseByReadFrequency(options.frequency);
        let df_plan_metrics = ExecutionPlanMetricsSet::new();
        let metrics = Metrics {
            metricsCollector: metrics_collector,
            ..Default::default()
        };

        AsyncParquetSstReader {
            path,
            objectStore: store,
            file_size_hint,
            num_rows_per_row_group: options.num_rows_per_row_group,
            projected_schema: options.projected_schema.clone(),
            meta_cache: options.meta_cache.clone(),
            predicate: options.predicate.clone(),
            frequency: options.frequency,
            meta_data: None,
            row_projector: None,
            metrics,
            df_plan_metrics,
        }
    }

    async fn parallelRead(&mut self, read_parallelism: usize) -> Result<Vec<RecordBatchWithKeyStream>> {
        assert!(read_parallelism > 0);

        self.init().await?;

        let subStreams = self.getRecordBatchStreams(read_parallelism).await?;

        if subStreams.is_empty() {
            return Ok(Vec::new());
        }

        let row_projector = ArrowRecordBatchProjector::from(self.row_projector.take().unwrap());

        // metadata must be inited after `init_if_necessary`.
        let sst_meta_data = self.meta_data.as_ref().unwrap().custom();

        // RecordBatchProjector 装饰 原始stream 装成stream的 fenquen
        let streams: Vec<_> = subStreams.into_iter().map(|stream| {
            Box::new(
                RecordBatchProjector::new(stream,
                                          row_projector.clone(),
                                          sst_meta_data.clone(),
                                          self.metrics.metricsCollector.clone())) as _
        }).collect();

        Ok(streams)
    }

    /// 得到的是1系列选中的row group的index
    fn pruneRowGroups(&self,
                      arrowSchema: SchemaRef,
                      row_groups: &[RowGroupMetaData],
                      parquet_filter: Option<&ParquetFilter>) -> Result<Vec<usize>> {
        let metricsCollector = self.metrics.metricsCollector.as_ref().map(|v| v.span(PRUNE_ROW_GROUPS_METRICS_COLLECTOR_NAME.to_string()));

        let mut rowGroupPruner =
            RowGroupPruner::new(&arrowSchema,
                                row_groups,
                                parquet_filter,
                                &self.predicate.exprs,
                                metricsCollector)?;

        Ok(rowGroupPruner.prune())
    }

    /// The final parallelism is ensured in the range: [1, num_row_groups].
    #[inline]
    fn decide_read_parallelism(suggested: usize, num_row_groups: usize) -> usize {
        suggested.min(num_row_groups).max(1)
    }

    fn buildRowSelection(&self,
                         arrowSchema: SchemaRef,
                         rowGroupIndexes: &[usize],
                         file_metadata: &parquet_ext::ParquetMetaData) -> Result<Option<RowSelection>> {
        let mergedExpr = match datafusion::optimizer::utils::conjunction(self.predicate.exprs.to_vec()) {
            Some(mergedExpr) => mergedExpr,
            None => return Ok(None),
        };

        // arrowSchema -> dfSchema -> physicalExpr
        let dfSchema = arrowSchema.clone().to_dfschema().context(DataFusionError)?;
        let physical_expr = physical_expr::create_physical_expr(&mergedExpr, &dfSchema, &arrowSchema, &ExecutionProps::new()).context(DataFusionError)?;
        let page_predicate = PagePruningPredicate::try_new(&physical_expr, arrowSchema.clone()).context(DataFusionError)?;

        // TODO: remove fixed partition
        let metrics = ParquetFileMetrics::new(0, self.path.as_ref(), &self.df_plan_metrics);

        page_predicate.prune(rowGroupIndexes, file_metadata, &metrics).context(DataFusionError)
    }

    // TODO: remove it and use the suggested api.
    #[allow(deprecated)]
    async fn getRecordBatchStreams(&mut self, suggested_parallelism: usize) -> Result<Vec<ArrowRecordBatchStream>> {
        assert!(self.meta_data.is_some());

        let meta_data = self.meta_data.as_ref().unwrap();
        let row_projector = self.row_projector.as_ref().unwrap();
        let arrowSchema = meta_data.custom().schema.arrow_schema.clone();

        // 牵涉到了parquet 得到的是1系列选中的row group的index
        let selectedRowGroupIndexes =
            self.pruneRowGroups(arrowSchema.clone(),
                                meta_data.parquet.row_groups(),
                                meta_data.custom.parquet_filter.as_ref())?;

        debug!("reader fetch record batches, path:{}, row_groups total:{}, after prune:{}",self.path,meta_data.parquet().num_row_groups(),selectedRowGroupIndexes.len());

        if selectedRowGroupIndexes.is_empty() {
            return Ok(Vec::new());
        }

        // 把selectedRowGroupIndexes通过了并行度切分 -----------------------------
        // Partition the batches by `read_parallelism`.
        let parallelism = suggested_parallelism.min(selectedRowGroupIndexes.len()).max(1);

        // TODO: we only support read parallelly when `batch_size` == `num_rows_per_row_group`, so this placing method is ok, we should adjust it when supporting it other situations.
        let chunk_size = selectedRowGroupIndexes.len() / parallelism;

        self.metrics.parallelism = parallelism;

        let mut target_row_group_chunks = vec![Vec::with_capacity(chunk_size); parallelism];

        for (index, rowGroupIndex) in selectedRowGroupIndexes.into_iter().enumerate() {
            target_row_group_chunks[index % parallelism].push(rowGroupIndex);
        }
        //----------------------------------------------------------------------------

        let projectionMask =
            ProjectionMask::leaves(meta_data.parquet.file_metadata().schema_descr(),
                                   row_projector.existed_source_projection().iter().copied());

        debug!("reader fetch record batches, parallelism suggest:{}, real:{}, chunk_size:{}, project:{:?}",suggested_parallelism, parallelism, chunk_size, projectionMask);

        let mut streams = Vec::with_capacity(target_row_group_chunks.len());

        for rowGroupIndexVec in target_row_group_chunks {
            let objectStoreReader =
                ObjectStoreReader::new(self.objectStore.clone(),
                                       self.path.clone(),
                                       meta_data.parquet.clone());

            let mut arrowReaderBuilder = ParquetRecordBatchStreamBuilder::new(objectStoreReader).await.with_context(|| ParquetError)?;

            let rowSelection = self.buildRowSelection(arrowSchema.clone(), &rowGroupIndexVec, &meta_data.parquet)?;

            debug!("build row selection for file path:{}, result:{:?}, page indexes:{}",self.path, rowSelection, meta_data.parquet.page_indexes().is_some());

            if let Some(rowSelection) = rowSelection {
                arrowReaderBuilder = arrowReaderBuilder.with_row_selection(rowSelection);
            };

            let stream = arrowReaderBuilder
                .with_batch_size(self.num_rows_per_row_group)
                .with_row_groups(rowGroupIndexVec)
                .with_projection(projectionMask.clone())
                .build()
                .with_context(|| ParquetError)?.map(|batch| batch.with_context(|| ParquetError));

            streams.push(Box::pin(stream) as _);
        }

        Ok(streams)
    }

    async fn init(&mut self) -> Result<()> {
        if self.meta_data.is_some() {
            return Ok(());
        }

        let meta_data = {
            let start = Instant::now();
            let meta_data = self.read_sst_meta().await?;
            self.metrics.read_meta_data_duration = start.elapsed();
            meta_data
        };

        let row_projector = self.projected_schema.tryProjectWithKey(&meta_data.custom().schema).box_err().context(Projection)?;

        self.meta_data = Some(meta_data);
        self.row_projector = Some(row_projector);

        Ok(())
    }

    async fn load_file_size(&self) -> Result<usize> {
        let file_size = match self.file_size_hint {
            Some(v) => v,
            None => {
                let object_meta = self.objectStore.head(self.path).await.context(ObjectStoreError)?;
                object_meta.size
            }
        };

        Ok(file_size)
    }

    fn need_update_cache(&self) -> bool {
        match self.frequency {
            ReadFrequency::Once => false,
            ReadFrequency::Frequent => true,
        }
    }

    async fn read_sst_meta(&mut self) -> Result<MetaData> {
        if let Some(cache) = &self.meta_cache {
            if let Some(meta_data) = cache.get(self.path.as_ref()) {
                self.metrics.meta_data_cache_hit = true;
                return Ok(meta_data);
            }
        }

        // The metadata can't be found in the cache, and let's fetch it from the storage.
        let avoid_update_cache = !self.need_update_cache();
        let empty_predicate = self.predicate.exprs().is_empty();

        let meta_data = {
            let ignore_sst_filter = avoid_update_cache && empty_predicate;
            self.load_meta_data_from_storage(ignore_sst_filter).await?
        };

        if avoid_update_cache || self.meta_cache.is_none() {
            return Ok(meta_data);
        }

        // Update the cache.
        self.meta_cache.as_ref().unwrap().put(self.path.to_string(), meta_data.clone());

        Ok(meta_data)
    }

    async fn load_meta_data_from_storage(&self, ignore_sst_filter: bool) -> Result<MetaData> {
        let file_size = self.load_file_size().await?;
        let chunk_reader_adapter = ChunkReaderAdapter::new(self.path, self.objectStore);

        let (parquet_meta_data, _) =
            parquet_ext::meta_data::fetch_parquet_metadata(file_size, &chunk_reader_adapter).await.with_context(|| FetchAndDecodeSstMeta { file_path: self.path.to_string()})?;

        // TODO: Support page index until https://github.com/CeresDB/ceresdb/issues/1040 is fixed.

        MetaData::try_new(&parquet_meta_data, ignore_sst_filter).box_err().context(DecodeSstMeta)
    }
}

struct RecordBatchReceiver {
    startSignalSender: Option<watch::Sender<()>>,
    receivers: Vec<Receiver<Result<RecordBatchWithKey>>>,
    currentReceiverIndex: usize,
    #[allow(dead_code)]
    drop_helper: AbortOnDropMany<()>,
}

#[async_trait]
impl PrefetchableStream for RecordBatchReceiver {
    type Item = Result<RecordBatchWithKey>;

    async fn start_prefetch(&mut self) {
        // Start the prefetch work in background when first poll is called.
        if let Some(startSignalSender) = self.startSignalSender.take() {
            if startSignalSender.send(()).is_err() {
                error!("The receiver for start prefetching has been closed");
            }
        }
    }

    async fn fetch_next(&mut self) -> Option<Self::Item> {
        self.next().await
    }
}

impl Stream for RecordBatchReceiver {
    type Item = Result<RecordBatchWithKey>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.receivers.is_empty() {
            return Poll::Ready(None);
        }

        // Start the prefetch work in background when first poll is called.
        if let Some(tx) = self.startSignalSender.take() {
            if tx.send(()).is_err() {
                error!("The receiver for start prefetching has been closed");
            }
        }

        let currentReceiverIndex = self.currentReceiverIndex;

        let currentReceiver = self.receivers.get_mut(currentReceiverIndex).unwrap_or_else(|| {
            panic!("currentReceiverIndex is impossible to be out-of-range")
        });

        match currentReceiver.poll_recv(cx) {
            // 如果单单瞧这里的话 会有担忧要是read中的是None怎能的办 这样的话整个的poll_next外部就不会去调用了 会影响到其它尚未耗尽的receiver 其实是不会这样 相见 sendSubStreamData 会阻拦这样的情形的 fenquen
            Poll::Ready(result) => {
                // If found `Poll::Pending`, we need to keep polling current rx until found `Poll::Ready` for ensuring the order of record batches,
                // because batches are placed into each stream by 轮询:
                // +------+    +------+    +------+
                // |  1   |    |  2   |    |  3   |
                // +------+    +------+    +------+
                // |  4   |    |  5   |    |  6   |
                // +------+    +------+    +------+
                // | ...  |    | ...  |    | ...  |
                // +------+    +------+    +------+

                // 起到了各个receiver轮询的效果
                self.currentReceiverIndex = (self.currentReceiverIndex + 1) % self.receivers.len();
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// spawn a new thread to read record_batches
pub struct ThreadedReader<'a> {
    asyncParquetSstReader: AsyncParquetSstReader<'a>,
    runtime: Arc<Runtime>,

    channel_cap: usize,
    read_parallelism: usize,
}

#[async_trait]
impl<'a> SstReader for ThreadedReader<'a> {
    async fn meta_data(&mut self) -> Result<SstMetaData> {
        self.asyncParquetSstReader.meta_data().await
    }

    async fn read(&mut self) -> Result<Box<dyn PrefetchableStream<Item=Result<RecordBatchWithKey>>>> {
        // get underlying sst readers and channels.
        let subStreamVec = self.asyncParquetSstReader.parallelRead(self.read_parallelism).await?;

        if subStreamVec.is_empty() {
            return Ok(Box::new(RecordBatchReceiver {
                startSignalSender: None,
                receivers: Vec::new(),
                currentReceiverIndex: 0,
                drop_helper: AbortOnDropMany(Vec::new()),
            }) as _);
        }

        let read_parallelism = subStreamVec.len();

        debug!("threadedReader read, suggest read_parallelism:{}, actual:{}",self.read_parallelism, read_parallelism);

        let (senders, receivers): (Vec<_>, Vec<_>) =
            (0..read_parallelism).map(|_| mpsc::channel::<Result<RecordBatchWithKey>>(self.channel_cap / read_parallelism)).unzip();

        let (startSignalSender, startSignalReceiver) = watch::channel(());

        // start the background readings.
        let mut handles = Vec::with_capacity(subStreamVec.len());

        for (stream, sender) in subStreamVec.into_iter().zip(senders.into_iter()) {
            let bg_prefetch_handle = self.sendSubStreamData(stream, sender, startSignalReceiver.clone());
            handles.push(bg_prefetch_handle);
        }

        // fenquen
        Ok(Box::new(RecordBatchReceiver {
            startSignalSender: Some(startSignalSender),
            receivers,
            currentReceiverIndex: 0,
            drop_helper: AbortOnDropMany(handles),
        }) as _)
    }
}


impl<'a> ThreadedReader<'a> {
    pub fn new(asyncParquetSstReader: AsyncParquetSstReader<'a>,
               runtime: Arc<Runtime>,
               read_parallelism: usize,
               channel_cap: usize) -> ThreadedReader {
        assert!(read_parallelism > 0, "read parallelism must be greater than 0");

        ThreadedReader {
            asyncParquetSstReader,
            runtime,
            channel_cap,
            read_parallelism,
        }
    }

    fn sendSubStreamData(&mut self,
                         mut stream: Box<dyn Stream<Item=Result<RecordBatchWithKey>> + Send + Unpin>,
                         sender: Sender<Result<RecordBatchWithKey>>,
                         mut rx: watch::Receiver<()>) -> JoinHandle<()> {
        self.runtime.spawn(async move {
            // Wait for the notification to start the bg prefetch work.
            if rx.changed().await.is_err() {
                error!("The prefetch notifier has been closed, exit the prefetch work");
                return;
            }

            // 之前有疑问 RecordBatchReceiver中会收拢多条自stream上的data 要是其中1个有none了岂不是其它的也会收到牵连也被强行停了 这里证明了不会这样 因为阻拦了none fenquen
            while let Some(batch) = stream.next().await {
                if let Err(e) = sender.send(batch).await {
                    error!("fail to send the fetched record batch result, err:{}", e);
                }
            }
        })
    }
}