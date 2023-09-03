// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table implementation

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use common_types::{
    row::{Row, RowGroupBuilder},
    schema::Schema,
    time::TimeRange,
};
use datafusion::{common::Column, logical_expr::Expr};
use future_ext::CancellationSafeFuture;
use futures::TryStreamExt;
use generic_error::BoxError;
use log::{error, warn};
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::{
    partition::PartitionInfo,
    predicate::PredicateBuilder,
    stream::{PartitionedStreams, SendableRecordBatchStream},
    table::{
        AlterOptions, AlterSchema, AlterSchemaRequest, Compact, Flush, FlushRequest, Get,
        GetInvalidPrimaryKey, GetNullPrimaryKey, GetRequest, MergeWrite, ReadOptions, ReadRequest,
        Result, Scan, Table, TableId, TableStats, TooManyPendingWrites, WaitForPendingWrites,
        Write, WriteRequest,
    },
    ANALYTIC_ENGINE_TYPE,
};
use tokio::sync::oneshot::{self, Receiver, Sender};
use trace_metric::MetricsCollector;

use self::data::TableDataRef;
use crate::{
    instance::{alter::Alterer, write::Writer, InstanceRef},
    space::{SpaceAndTable, SpaceRef},
};
use crate::instance::TableEngineInstance;
use crate::table::data::TableData;

pub mod data;
pub mod metrics;
pub mod sst_util;
pub mod version;
pub mod version_edit;

const GET_METRICS_COLLECTOR_NAME: &str = "get";
// Additional 1/10 of the pending writes capacity is reserved for new pending
// writes.
const ADDITIONAL_PENDING_WRITE_CAP_RATIO: usize = 10;

struct WriteRequests {
    pub space: SpaceRef,
    pub table_data: TableDataRef,
    pub instance: InstanceRef,
    pub pending_writes: Arc<Mutex<PendingWriteQueue>>,
}

impl WriteRequests {
    pub fn new(instance: InstanceRef,
               space: SpaceRef,
               table_data: TableDataRef,
               pending_writes: Arc<Mutex<PendingWriteQueue>>) -> Self {
        Self {
            space,
            instance,
            table_data,
            pending_writes,
        }
    }
}

pub struct TableImpl {
    space: SpaceRef,
    tableEngineInstance: Arc<TableEngineInstance>,
    engine_type: String,
    /// Holds a strong reference to prevent the underlying table from being dropped when this handle exist.
    tableData: Arc<TableData>,
    /// Buffer for written rows.
    pending_writes: Arc<Mutex<PendingWriteQueue>>,
}

impl TableImpl {
    pub fn new(instance: Arc<TableEngineInstance>, space_table: SpaceAndTable) -> Self {
        let pending_writes = Mutex::new(PendingWriteQueue::new(instance.max_rows_in_write_queue));
        let table_data = space_table.table_data().clone();
        let space = space_table.space().clone();
        Self {
            space,
            tableEngineInstance: instance,
            engine_type: ANALYTIC_ENGINE_TYPE.to_string(),
            tableData: table_data,
            pending_writes: Arc::new(pending_writes),
        }
    }
}

impl fmt::Debug for TableImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableImpl")
            .field("space_id", &self.space.id)
            .field("table_id", &self.tableData.id)
            .finish()
    }
}

/// The queue for buffering pending write requests.
struct PendingWriteQueue {
    max_rows: usize,
    pending_writes: PendingWrites,
}

/// The underlying queue for buffering pending write requests.
#[derive(Default)]
struct PendingWrites {
    writes: Vec<WriteRequest>,
    notifiers: Vec<Sender<Result<()>>>,
    num_rows: usize,
}

impl PendingWrites {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            writes: Vec::with_capacity(cap),
            notifiers: Vec::with_capacity(cap),
            num_rows: 0,
        }
    }

    /// Try to push the request into the pending queue.
    ///
    /// This push will be rejected if the schema is different.
    fn try_push(&mut self, request: WriteRequest) -> QueueResult {
        if !self.is_same_schema(request.rowGroup.schema()) {
            return QueueResult::Reject(request);
        }

        // For the first pending writes, don't provide the receiver because it should do
        // the write for the pending writes and no need to wait for the notification.
        let res = if self.is_empty() {
            QueueResult::First
        } else {
            let (tx, rx) = oneshot::channel();
            self.notifiers.push(tx);
            QueueResult::Waiter(rx)
        };

        self.num_rows += request.rowGroup.num_rows();
        self.writes.push(request);

        res
    }

    /// Check if the schema of the request is the same as the schema of the
    /// pending write requests.
    ///
    /// Return true if the pending write requests is empty.
    fn is_same_schema(&self, schema: &Schema) -> bool {
        if self.is_empty() {
            return true;
        }

        let request = &self.writes[0];
        schema.version() == request.rowGroup.schema().version()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }
}

/// The result when trying to push write request to the queue.
enum QueueResult {
    /// This request is rejected because the queue is full or the schema is
    /// different.
    Reject(WriteRequest),
    /// This request is the first one in the queue.
    First,
    /// This request is pushed into the queue and the caller should wait for the
    /// finish notification.
    Waiter(Receiver<Result<()>>),
}

impl PendingWriteQueue {
    fn new(max_rows: usize) -> Self {
        Self {
            max_rows,
            pending_writes: PendingWrites::default(),
        }
    }

    /// Try to push the request into the queue.
    ///
    /// If the queue is full or the schema is different, return the request
    /// back. Otherwise, return a receiver to let the caller wait for the write
    /// result.
    fn try_push(&mut self, request: WriteRequest) -> QueueResult {
        if self.is_full() {
            return QueueResult::Reject(request);
        }

        self.pending_writes.try_push(request)
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.pending_writes.num_rows >= self.max_rows
    }

    /// Clear the pending writes and reset the number of rows.
    fn take_pending_writes(&mut self) -> PendingWrites {
        let curr_num_reqs = self.pending_writes.writes.len();
        let new_cap = curr_num_reqs / ADDITIONAL_PENDING_WRITE_CAP_RATIO + curr_num_reqs;
        let new_pending_writes = PendingWrites::with_capacity(new_cap);
        std::mem::replace(&mut self.pending_writes, new_pending_writes)
    }
}

/// Merge the pending write requests into a same one.
///
/// The schema of all the pending write requests should be the same.
/// REQUIRES: the `pending_writes` is required non-empty.
fn merge_pending_write_requests(
    mut pending_writes: Vec<WriteRequest>,
    num_pending_rows: usize,
) -> WriteRequest {
    assert!(!pending_writes.is_empty());

    let mut last_req = pending_writes.pop().unwrap();
    let last_rows = last_req.rowGroup.take_rows();
    let schema = last_req.rowGroup.into_schema();
    let mut row_group_builder = RowGroupBuilder::with_capacity(schema, num_pending_rows);

    for mut pending_req in pending_writes {
        let rows = pending_req.rowGroup.take_rows();
        for row in rows {
            row_group_builder.push_checked_row(row)
        }
    }
    for row in last_rows {
        row_group_builder.push_checked_row(row);
    }
    let row_group = row_group_builder.build();
    WriteRequest { rowGroup: row_group }
}

impl TableImpl {
    /// Perform table write with pending queue.
    ///
    /// The writes will be put into the pending queue first. And the writer who
    /// submits the first request to the queue is responsible for merging and
    /// writing all the writes in the queue.
    ///
    /// NOTE: The write request will be rejected if the queue is full.
    async fn write_with_pending_queue(&self, request: WriteRequest) -> Result<usize> {
        let num_rows = request.rowGroup.num_rows();

        // Failed to acquire the serial_exec, put the request into the
        // pending queue.
        let queue_res = {
            let mut pending_queue = self.pending_writes.lock().unwrap();
            pending_queue.try_push(request)
        };

        match queue_res {
            QueueResult::First => {
                // This is the first request in the queue, and we should
                // take responsibilities for merging and writing the
                // requests in the queue.
                let write_requests = WriteRequests::new(
                    self.tableEngineInstance.clone(),
                    self.space.clone(),
                    self.tableData.clone(),
                    self.pending_writes.clone(),
                );

                match CancellationSafeFuture::new(
                    Self::write_requests(write_requests),
                    self.tableEngineInstance.getWriteRunTime().clone(),
                )
                    .await
                {
                    Ok(_) => Ok(num_rows),
                    Err(e) => Err(e),
                }
            }
            QueueResult::Waiter(rx) => {
                // The request is successfully pushed into the queue, and just wait for the
                // write result.
                match rx.await {
                    Ok(res) => {
                        res.box_err().context(Write { table: self.name() })?;
                        Ok(num_rows)
                    }
                    Err(_) => WaitForPendingWrites { table: self.name() }.fail(),
                }
            }
            QueueResult::Reject(_) => {
                // The queue is full, return error.
                error!(
                    "Pending_writes queue is full, max_rows_in_queue:{}, table:{}",
                    self.tableEngineInstance.max_rows_in_write_queue,
                    self.name(),
                );
                TooManyPendingWrites { table: self.name() }.fail()
            }
        }
    }

    async fn write_requests(write_requests: WriteRequests) -> Result<()> {
        let mut serial_exec = write_requests.table_data.tableOpSerialExecutor.lock().await;
        // The `serial_exec` is acquired, let's merge the pending requests and write
        // them all.
        let pending_writes = {
            let mut pending_queue = write_requests.pending_writes.lock().unwrap();
            pending_queue.take_pending_writes()
        };
        assert!(
            !pending_writes.is_empty(),
            "The pending writes should contain at least the one just pushed."
        );
        let merged_write_request =
            merge_pending_write_requests(pending_writes.writes, pending_writes.num_rows);

        let mut writer = Writer::new(
            write_requests.instance,
            write_requests.space,
            write_requests.table_data.clone(),
            &mut serial_exec,
        );
        let write_res = writer
            .write(merged_write_request)
            .await
            .box_err()
            .context(Write {
                table: write_requests.table_data.name.clone(),
            });

        // There is no waiter for pending writes, return the write result.
        let notifiers = pending_writes.notifiers;
        if notifiers.is_empty() {
            return Ok(());
        }

        // Notify the waiters for the pending writes.
        match write_res {
            Ok(_) => {
                for notifier in notifiers {
                    if notifier.send(Ok(())).is_err() {
                        warn!(
                            "Failed to notify the ok result of pending writes, table:{}",
                            write_requests.table_data.name
                        );
                    }
                }
                Ok(())
            }
            Err(e) => {
                let err_msg = format!("Failed to do merge write, err:{e}");
                for notifier in notifiers {
                    let err = MergeWrite { msg: &err_msg }.fail();
                    if notifier.send(err).is_err() {
                        warn!(
                            "Failed to notify the error result of pending writes, table:{}",
                            write_requests.table_data.name
                        );
                    }
                }
                Err(e)
            }
        }
    }

    #[inline]
    fn should_queue_write_request(&self, request: &WriteRequest) -> bool {
        request.rowGroup.num_rows() < self.tableEngineInstance.max_rows_in_write_queue
    }
}

#[async_trait]
impl Table for TableImpl {
    fn name(&self) -> &str {
        &self.tableData.name
    }

    fn id(&self) -> TableId {
        self.tableData.id
    }

    fn schema(&self) -> Schema {
        self.tableData.schema()
    }

    fn options(&self) -> HashMap<String, String> {
        self.tableData.table_options().to_raw_map()
    }

    fn partition_info(&self) -> Option<PartitionInfo> {
        None
    }

    fn engine_type(&self) -> &str {
        &self.engine_type
    }

    fn stats(&self) -> TableStats {
        self.tableData.metrics.table_stats()
    }

    async fn write(&self, writeRequest: WriteRequest) -> Result<usize> {
        let _timer = self.tableData.metrics.start_table_total_timer();

        // 默认情况是不会满足的
        if self.should_queue_write_request(&writeRequest) {
            return self.write_with_pending_queue(writeRequest).await;
        }

        let mut serial_exec = self.tableData.tableOpSerialExecutor.lock().await;

        let mut writer =
            Writer::new(self.tableEngineInstance.clone(),
                        self.space.clone(),
                        self.tableData.clone(),
                        &mut serial_exec, );

        writer.write(writeRequest).await.box_err().context(Write { table: self.name() })
    }

    async fn read(&self, mut readRequest: ReadRequest) -> Result<SendableRecordBatchStream> {
        readRequest.readOptions.read_parallelism = 1;

        let mut partitionedStreams =
            self.tableEngineInstance.partitionedRead(&self.tableData, readRequest).await.box_err().context(Scan { table: self.name() })?;

        assert_eq!(partitionedStreams.streams.len(), 1);

        Ok(partitionedStreams.streams.pop().unwrap())
    }

    async fn get(&self, request: GetRequest) -> Result<Option<Row>> {
        let schema = request.projected_schema.to_record_schema_with_key();
        let primary_key_columns = &schema.key_columns()[..];
        ensure!(
            primary_key_columns.len() == request.primary_key.len(),
            GetInvalidPrimaryKey {
                schema: schema.clone(),
                primary_key_columns,
            }
        );

        let mut primary_key_exprs: Vec<Expr> = Vec::with_capacity(request.primary_key.len());
        for (primary_key_value, column_schema) in
        request.primary_key.iter().zip(primary_key_columns.iter())
        {
            let v = primary_key_value
                .as_scalar_value()
                .with_context(|| GetNullPrimaryKey {
                    schema: schema.clone(),
                    primary_key_columns,
                })?;
            primary_key_exprs.push(
                Expr::Column(Column::from_qualified_name(&column_schema.name)).eq(Expr::Literal(v)),
            );
        }

        let predicate = PredicateBuilder::default()
            .set_time_range(TimeRange::min_to_max())
            .add_pushdown_exprs(&primary_key_exprs)
            .build();

        let read_request = ReadRequest {
            request_id: request.request_id,
            readOptions: ReadOptions::default(),
            projectedSchema: request.projected_schema,
            predicate,
            metrics_collector: MetricsCollector::new(GET_METRICS_COLLECTOR_NAME.to_string()),
        };
        let mut batch_stream = self
            .read(read_request)
            .await
            .box_err()
            .context(Scan { table: self.name() })?;

        let mut result_columns = Vec::with_capacity(schema.num_columns());

        while let Some(batch) = batch_stream
            .try_next()
            .await
            .box_err()
            .context(Get { table: self.name() })?
        {
            let row_num = batch.num_rows();
            if row_num == 0 {
                return Ok(None);
            }
            for row_idx in 0..row_num {
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    result_columns.push(col.datum(row_idx));
                }

                let mut result_columns_k = vec![];
                for col_idx in schema.primary_key_idx() {
                    result_columns_k.push(result_columns[*col_idx].clone());
                }
                if request.primary_key == result_columns_k {
                    return Ok(Some(Row::from_datums(result_columns)));
                }
                result_columns.clear();
            }
        }

        Ok(None)
    }

    async fn partitionedRead(&self, readRequest: ReadRequest) -> Result<PartitionedStreams> {
        let streams = self.tableEngineInstance.partitionedRead(&self.tableData, readRequest).await.box_err().context(Scan { table: self.name() })?;
        Ok(streams)
    }

    async fn alter_schema(&self, request: AlterSchemaRequest) -> Result<usize> {
        let mut serial_exec = self.tableData.tableOpSerialExecutor.lock().await;
        let mut alterer =
            Alterer::new(self.tableData.clone(),
                         &mut serial_exec,
                         self.tableEngineInstance.clone()).await;

        alterer.alter_schema_of_table(request).await.box_err().context(AlterSchema { table: self.name() })?;
        Ok(0)
    }

    async fn alter_options(&self, options: HashMap<String, String>) -> Result<usize> {
        let mut serial_exec = self.tableData.tableOpSerialExecutor.lock().await;
        let alterer =
            Alterer::new(self.tableData.clone(),
                         &mut serial_exec,
                         self.tableEngineInstance.clone()).await;

        alterer.alter_options_of_table(options).await.box_err().context(AlterOptions { table: self.name() })?;
        Ok(0)
    }

    async fn flush(&self, request: FlushRequest) -> Result<()> {
        self.tableEngineInstance.manual_flush_table(&self.tableData, request).await.box_err().context(Flush { table: self.name() })
    }

    async fn compact(&self) -> Result<()> {
        self.tableEngineInstance.manual_compact_table(&self.tableData).await.box_err().context(Compact { table: self.name() })?;
        Ok(())
    }
}