// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    time::{Duration, Instant},
};
use std::sync::Arc;

use async_trait::async_trait;
use common_types::{
    projected_schema::ProjectedSchema, record_batch::RecordBatchWithKey, request_id::RequestId,
    schema::RecordSchemaWithKey,
};
use generic_error::GenericError;
use log::debug;
use macros::define_result;
use snafu::{ResultExt, Snafu};
use table_engine::{predicate::PredicateRef, table::TableId};
use trace_metric::{MetricsCollector, TraceMetricWhenDrop};

use crate::{
    row_iter::{
        record_batch_stream, record_batch_stream::BoxedPrefetchableRecordBatchStream,
        RecordBatchWithKeyIterator,
    },
    space::SpaceId,
    sst::{
        factory::SstReadOptions,
        file::FileHandle,
    },
    table::version::{MemTableVec, SamplingMemTable},
};
use crate::sst::factory::{ObjectStoreChooser, SstFactory};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Fail to build stream from the memtable, err:{}", source))]
    BuildStreamFromMemtable {
        source: crate::row_iter::record_batch_stream::Error,
    },

    #[snafu(display("Fail to build stream from the sst file, err:{}", source))]
    BuildStreamFromSst {
        source: crate::row_iter::record_batch_stream::Error,
    },

    #[snafu(display("Fail to poll next record batch, err:{}", source))]
    PollNextRecordBatch { source: GenericError },
}

define_result!(Error);

/// Required parameters to construct the [Builder].
#[derive(Clone, Debug)]
pub struct ChainConfig<'a> {
    pub request_id: RequestId,
    pub metrics_collector: Option<MetricsCollector>,
    pub deadline: Option<Instant>,
    pub space_id: SpaceId,
    pub table_id: TableId,
    /// The projected schema to read.
    pub projected_schema: ProjectedSchema,
    /// Predicate of the query.
    pub predicate: PredicateRef,
    pub num_streams_to_prefetch: usize,

    pub sst_read_options: SstReadOptions,
    /// Sst factory
    pub sst_factory: &'a Arc<dyn SstFactory>,
    /// Store picker for persisting sst.
    pub store_picker: &'a Arc<dyn ObjectStoreChooser>,
}

/// Builder for [ChainIterator].
#[must_use]
pub struct Builder<'a> {
    config: ChainConfig<'a>,
    /// Sampling memtable to read.
    sampling_mem: Option<SamplingMemTable>,
    memtables: MemTableVec,
    ssts: Vec<Vec<FileHandle>>,
}

impl<'a> Builder<'a> {
    pub fn new(config: ChainConfig<'a>) -> Self {
        Self {
            config,
            sampling_mem: None,
            memtables: Vec::new(),
            ssts: Vec::new(),
        }
    }

    pub fn sampling_mem(mut self, sampling_mem: Option<SamplingMemTable>) -> Self {
        self.sampling_mem = sampling_mem;
        self
    }

    pub fn memtables(mut self, memtables: MemTableVec) -> Self {
        self.memtables = memtables;
        self
    }

    pub fn ssts(mut self, ssts: Vec<Vec<FileHandle>>) -> Self {
        self.ssts = ssts;
        self
    }
}

impl<'a> Builder<'a> {
    pub async fn build(self) -> Result<ChainIterator> {
        let total_sst_streams: usize = self.ssts.iter().map(|v| v.len()).sum();
        let mut total_streams = self.memtables.len() + total_sst_streams;
        if self.sampling_mem.is_some() {
            total_streams += 1;
        }
        let mut streams = Vec::with_capacity(total_streams);

        if let Some(v) = &self.sampling_mem {
            let stream = record_batch_stream::filteredStreamFromMemTable(
                self.config.projected_schema.clone(),
                false,
                &v.mem,
                false,
                self.config.predicate.as_ref(),
                self.config.deadline,
                self.config.metrics_collector.clone(),
            )
            .context(BuildStreamFromMemtable)?;
            streams.push(stream);
        }

        for memtable in &self.memtables {
            let stream = record_batch_stream::filteredStreamFromMemTable(
                self.config.projected_schema.clone(),
                false,
                // chain iterator only handle the case reading in no order so just read in asc
                // order by default.
                &memtable.memTable,
                false,
                self.config.predicate.as_ref(),
                self.config.deadline,
                self.config.metrics_collector.clone(),
            )
            .context(BuildStreamFromMemtable)?;
            streams.push(stream);
        }

        for leveled_ssts in &self.ssts {
            for sst in leveled_ssts {
                let stream = record_batch_stream::filteredStreamFromSst(
                    self.config.space_id,
                    self.config.table_id,
                    sst,
                    self.config.sst_factory,
                    &self.config.sst_read_options,
                    self.config.store_picker,
                    self.config.metrics_collector.clone(),
                ).await.context(BuildStreamFromSst)?;

                streams.push(stream);
            }
        }

        debug!(
            "Build chain iterator, table_id:{:?}, request_id:{}, memtables:{:?}, ssts:{:?}",
            self.config.table_id, self.config.request_id, self.memtables, self.ssts
        );

        Ok(ChainIterator {
            space_id: self.config.space_id,
            table_id: self.config.table_id,
            request_id: self.config.request_id,
            schema: self.config.projected_schema.to_record_schema_with_key(),
            streams,
            num_streams_to_prefetch: self.config.num_streams_to_prefetch,
            ssts: self.ssts,
            next_stream_idx: 0,
            next_prefetch_stream_idx: 0,
            inited_at: None,
            created_at: Instant::now(),
            metrics: Metrics::new(
                self.memtables.len(),
                total_sst_streams,
                self.config.metrics_collector.clone(),
            ),
        })
    }
}

/// Metrics for [ChainIterator].
#[derive(TraceMetricWhenDrop)]
struct Metrics {
    #[metric(number)]
    num_memtables: usize,
    #[metric(number)]
    num_ssts: usize,
    /// Total batch fetched.
    #[metric(number)]
    total_batch_fetched: usize,
    /// Total rows fetched.
    #[metric(number)]
    total_rows_fetched: usize,
    /// Create time of the metrics.
    #[metric(duration)]
    since_create: Duration,
    /// Inited time of the iterator.
    #[metric(duration)]
    since_init: Duration,
    /// Actual scan duration.
    #[metric(duration)]
    scan_duration: Duration,
    #[metric(collector)]
    metrics_collector: Option<MetricsCollector>,
}

impl Metrics {
    fn new(
        num_memtables: usize,
        num_ssts: usize,
        metrics_collector: Option<MetricsCollector>,
    ) -> Self {
        Self {
            num_memtables,
            num_ssts,
            total_batch_fetched: 0,
            total_rows_fetched: 0,
            since_create: Duration::default(),
            since_init: Duration::default(),
            scan_duration: Duration::default(),
            metrics_collector,
        }
    }
}

impl fmt::Debug for Metrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Metrics")
            .field("num_memtables", &self.num_memtables)
            .field("num_ssts", &self.num_ssts)
            .field("total_batch_fetched", &self.total_batch_fetched)
            .field("total_rows_fetched", &self.total_rows_fetched)
            .field("duration_since_create", &self.since_create)
            .field("duration_since_init", &self.since_init)
            .field("scan_duration", &self.scan_duration)
            .finish()
    }
}

/// ChainIter chains memtables and ssts and reads the [RecordBatch] from them batch by batch.
///
/// Note: The chain order is `memtable -> sst level 0 -> sst_level 1`.
pub struct ChainIterator {
    space_id: SpaceId,
    table_id: TableId,
    request_id: RequestId,
    schema: RecordSchemaWithKey,
    streams: Vec<BoxedPrefetchableRecordBatchStream>,
    num_streams_to_prefetch: usize,
    /// ssts are kept here to avoid them from being purged.
    #[allow(dead_code)]
    ssts: Vec<Vec<FileHandle>>,
    /// The range of the index is [0, streams.len()] and the iterator is
    /// exhausted if it reaches `streams.len()`.
    next_stream_idx: usize,
    next_prefetch_stream_idx: usize,

    inited_at: Option<Instant>,
    created_at: Instant,
    /// metrics for the iterator.
    metrics: Metrics,
}

impl ChainIterator {
    fn init_if_necessary(&mut self) {
        if self.inited_at.is_some() {
            return;
        }
        self.inited_at = Some(Instant::now());

        debug!("Init ChainIterator, space_id:{}, table_id:{:?}, request_id:{}, total_streams:{}, schema:{:?}",
            self.space_id, self.table_id, self.request_id, self.streams.len(), self.schema
        );
    }

    /// Maybe prefetch the necessary stream for future reading.
    async fn maybe_prefetch(&mut self) {
        while self.next_prefetch_stream_idx < self.next_stream_idx + self.num_streams_to_prefetch
            && self.next_prefetch_stream_idx < self.streams.len()
        {
            self.streams[self.next_prefetch_stream_idx]
                .start_prefetch()
                .await;
            self.next_prefetch_stream_idx += 1;
        }
    }

    async fn next_batch_internal(&mut self) -> Result<Option<RecordBatchWithKey>> {
        self.init_if_necessary();
        self.maybe_prefetch().await;

        while self.next_stream_idx < self.streams.len() {
            let read_stream = &mut self.streams[self.next_stream_idx];
            let sequenced_record_batch = read_stream
                .fetch_next()
                .await
                .transpose()
                .context(PollNextRecordBatch)?;

            match sequenced_record_batch {
                Some(v) => {
                    self.metrics.total_rows_fetched += v.num_rows();
                    self.metrics.total_batch_fetched += 1;

                    if v.num_rows() > 0 {
                        return Ok(Some(v.recordBatchWithKey));
                    }
                }
                // Fetch next stream only if the current sequence_record_batch is None.
                None => {
                    self.next_stream_idx += 1;
                    self.maybe_prefetch().await;
                }
            }
        }

        self.metrics.since_create = self.created_at.elapsed();
        self.metrics.since_init = self
            .inited_at
            .as_ref()
            .map(|v| v.elapsed())
            .unwrap_or_default();

        Ok(None)
    }
}

impl Drop for ChainIterator {
    fn drop(&mut self) {
        debug!(
            "Chain iterator dropped, space_id:{}, table_id:{:?}, request_id:{}, inited_at:{:?}, metrics:{:?}",
            self.space_id, self.table_id, self.request_id, self.inited_at, self.metrics,
        );
    }
}

#[async_trait]
impl RecordBatchWithKeyIterator for ChainIterator {
    type Error = Error;

    fn schema(&self) -> &RecordSchemaWithKey {
        &self.schema
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatchWithKey>> {
        let timer = Instant::now();
        let res = self.next_batch_internal().await;
        self.metrics.scan_duration += timer.elapsed();

        res
    }
}