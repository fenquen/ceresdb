// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Write logic of instance

use std::sync::Arc;
use bytes_ext::ByteVec;
use ceresdbproto::{schema as schema_pb, table_requests};
use codec::row;
use common_types::{
    row::{RowGroup, RowGroupSlicer},
    schema::{IndexInWriterSchema, Schema},
};
use log::{debug, error, info, trace, warn};
use macros::define_result;
use smallvec::SmallVec;
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use table_engine::table::WriteRequest;
use wal::{
    kv_encoder::LogBatchEncoder,
    manager::{SequenceNumber, WalLocation, WriteContext},
};

use crate::{
    instance,
    instance::{
        flush_compaction::TableFlushOptions, serial_executor::TableOpSerialExecutor,
    },
    memtable::{key::KeySequence, PutContext},
    payload::WritePayload,
    table::{data::TableDataRef, version::MemTableForWrite},
};
use crate::instance::TableEngineInstance;
use crate::space::Space;
use crate::table::data::TableData;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to encode payloads, table:{}, wal_location:{:?}, err:{}", table, wal_location, source))]
    EncodePayloads {
        table: String,
        wal_location: WalLocation,
        source: wal::manager::Error,
    },

    #[snafu(display("Failed to write to wal, table:{}, err:{}", table, source))]
    WriteLogBatch {
        table: String,
        source: wal::manager::Error,
    },

    #[snafu(display("Failed to write to memtable, table:{}, err:{}", table, source))]
    WriteMemTable {
        table: String,
        source: crate::table::version::Error,
    },

    #[snafu(display("Try to write to a dropped table, table:{}", table))]
    WriteDroppedTable { table: String },

    #[snafu(display("too many rows to write in a single round (more than {}), table:{}, rows:{}.\nBacktrace:\n{}", MAX_ROWS_TO_WRITE, table, rows, backtrace, ))]
    TooManyRows {
        table: String,
        rows: usize,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to find mutable memtable, table:{}, err:{}", table, source))]
    FindMutableMemTable {
        table: String,
        source: crate::table::data::Error,
    },

    #[snafu(display("Failed to flush table, table:{}, err:{}", table, source))]
    FlushTable {
        table: String,
        source: crate::instance::flush_compaction::Error,
    },

    #[snafu(display(
    "Background flush failed, cannot write more data, err:{}.\nBacktrace:\n{}",
    msg,
    backtrace
    ))]
    BackgroundFlushFailed { msg: String, backtrace: Backtrace },

    #[snafu(display("Schema of request is incompatible with table, err:{}", source))]
    IncompatSchema {
        source: common_types::schema::CompatError,
    },

    #[snafu(display("Failed to encode row group, err:{}", source))]
    EncodeRowGroup { source: codec::row::Error },

    #[snafu(display("Failed to update sequence of memtable, err:{}", source))]
    UpdateMemTableSequence { source: crate::memtable::Error },
}

define_result!(Error);

/// Max rows in a write request, must less than [u32::MAX]
const MAX_ROWS_TO_WRITE: usize = 10_000_000;

pub(crate) struct EncodeContext {
    pub rowGroup: RowGroup,
    pub indexInWriterSchema: IndexInWriterSchema,
    pub encodedRows: Vec<Vec<u8>>,
}

impl EncodeContext {
    pub fn new(rowGroup: RowGroup) -> Self {
        Self {
            rowGroup,
            indexInWriterSchema: IndexInWriterSchema::default(),
            encodedRows: Vec::new(),
        }
    }

    pub fn encode_rows(&mut self, table_schema: &Schema) -> Result<()> {
        row::encode_row_group_for_wal(
            &self.rowGroup,
            table_schema,
            &self.indexInWriterSchema,
            &mut self.encodedRows,
        ).context(EncodeRowGroup)?;

        assert_eq!(self.rowGroup.num_rows(), self.encodedRows.len());

        Ok(())
    }
}

/// Split the write request into multiple batches whose size is determined by the `max_bytes_per_batch`.
struct WriteRowGroupSplitter {
    /// Max bytes per batch. Actually, the size of a batch is not exactly
    /// ensured less than this `max_bytes_per_batch`, but it is guaranteed that
    /// the batch contains at most one more row when its size exceeds this `max_bytes_per_batch`.
    max_bytes_per_batch: usize,
}

enum SplitResult<'a> {
    Splitted {
        encoded_batches: Vec<Vec<ByteVec>>,
        row_group_batches: Vec<RowGroupSlicer<'a>>,
    },
    Integrate {
        encodedRows: Vec<ByteVec>,
        rowGroupSlicer: RowGroupSlicer<'a>,
    },
}

impl WriteRowGroupSplitter {
    pub fn new(max_bytes_per_batch: usize) -> Self {
        Self {
            max_bytes_per_batch,
        }
    }

    /// Split the write request into multiple batches.
    ///
    /// NOTE: The length of the `encoded_rows` should be the same as the number
    /// of rows in the `row_group`.
    pub fn split<'a>(
        &'_ self,
        encoded_rows: Vec<ByteVec>,
        row_group: &'a RowGroup,
    ) -> SplitResult<'a> {
        let end_row_indexes = self.compute_batches(&encoded_rows);
        if end_row_indexes.len() <= 1 {
            // No need to split.
            return SplitResult::Integrate {
                encodedRows: encoded_rows,
                rowGroupSlicer: RowGroupSlicer::from(row_group),
            };
        }

        let mut prev_end_row_index = 0;
        let mut encoded_batches = Vec::with_capacity(end_row_indexes.len());
        let mut row_group_batches = Vec::with_capacity(end_row_indexes.len());
        for end_row_index in &end_row_indexes {
            let end_row_index = *end_row_index;
            let curr_batch = Vec::with_capacity(end_row_index - prev_end_row_index);
            encoded_batches.push(curr_batch);
            let row_group_slicer =
                RowGroupSlicer::new(prev_end_row_index..end_row_index, row_group);
            row_group_batches.push(row_group_slicer);

            prev_end_row_index = end_row_index;
        }

        let mut current_batch_idx = 0;
        for (row_idx, encoded_row) in encoded_rows.into_iter().enumerate() {
            if row_idx >= end_row_indexes[current_batch_idx] {
                current_batch_idx += 1;
            }
            encoded_batches[current_batch_idx].push(encoded_row);
        }

        SplitResult::Splitted {
            encoded_batches,
            row_group_batches,
        }
    }

    /// Compute the end row indexes in the original `encoded_rows` of each batch.
    fn compute_batches(&self, encoded_rows: &[ByteVec]) -> Vec<usize> {
        let mut current_batch_size = 0;
        let mut end_row_indexes = Vec::new();
        for (row_idx, encoded_row) in encoded_rows.iter().enumerate() {
            let row_size = encoded_row.len();
            current_batch_size += row_size;

            // If the current batch size exceeds the `max_bytes_per_batch`, freeze this
            // batch by recording its end row index.
            // Note that such check may cause the batch size exceeds the
            // `max_bytes_per_batch`.
            if current_batch_size >= self.max_bytes_per_batch {
                current_batch_size = 0;
                end_row_indexes.push(row_idx + 1)
            }
        }

        if current_batch_size > 0 {
            end_row_indexes.push(encoded_rows.len());
        }

        end_row_indexes
    }
}

pub struct Writer<'a> {
    instance: Arc<TableEngineInstance>,
    space: Arc<Space>,
    tableData: Arc<TableData>,
    serial_exec: &'a mut TableOpSerialExecutor,
}

impl<'a> Writer<'a> {
    pub fn new(instance: Arc<TableEngineInstance>,
               space: Arc<Space>,
               table_data: Arc<TableData>,
               serial_exec: &'a mut TableOpSerialExecutor) -> Writer<'a> {
        // ensure the writer has permission to handle the write of the table.
        assert_eq!(table_data.id, serial_exec.table_id());

        Writer {
            instance,
            space,
            tableData: table_data,
            serial_exec,
        }
    }
}

pub(crate) struct MemTableWriter<'a> {
    tableData: TableDataRef,
    _serial_exec: &'a mut TableOpSerialExecutor,
}

impl<'a> MemTableWriter<'a> {
    pub fn new(tableData: TableDataRef, serial_exec: &'a mut TableOpSerialExecutor) -> Self {
        Self {
            tableData,
            _serial_exec: serial_exec,
        }
    }

    // TODO(yingwen): How to trigger flush if we found memtables are full during inserting memtable? RocksDB checks memtable size in MemTableInserter
    /// Write data into memtable.
    ///
    /// index_in_writer must match the schema in table_data.
    pub fn write(&self,
                 sequenceNumber: SequenceNumber,
                 rowGroupSlicer: &RowGroupSlicer,
                 indexInWriterSchema: IndexInWriterSchema) -> Result<()> {
        let _timer = self.tableData.metrics.start_table_write_memtable_timer();

        if rowGroupSlicer.is_empty() {
            return Ok(());
        }

        let schema = &self.tableData.schema();

        // store all memtables we wrote and update their last sequence later
        let mut memTableForWriteVec: SmallVec<[_; 4]> = SmallVec::new();
        let mut lastMemTableForWrite: Option<MemTableForWrite> = None;

        let mut ctx = PutContext::new(indexInWriterSchema);

        for (rowIndex, row) in rowGroupSlicer.iter().enumerate() {
            // TODO(yingwen): Add RowWithSchema and take RowWithSchema as input, then remove this unwrap()
            let timestamp = row.timestamp(schema).unwrap();

            // skip expired row
            if self.tableData.is_expired(timestamp) {
                trace!("skip expired row when write to memtable, row:{:?}", row);
                continue;
            }

            if lastMemTableForWrite.is_none() || !lastMemTableForWrite.as_ref().unwrap().acceptTimestamp(timestamp) {
                // The time range is not processed by current memtable, find next one.
                let memTableForWrite =
                    self.tableData.findOrCreateMutable(timestamp, schema).context(FindMutableMemTable { table: &self.tableData.name })?;

                memTableForWriteVec.push(memTableForWrite.clone());

                lastMemTableForWrite = Some(memTableForWrite);
            }

            // we have check the row num is less than `MAX_ROWS_TO_WRITE`, it is safe to cast it to u32 here
            let keySequence = KeySequence::new(sequenceNumber, rowIndex as u32);

            // TODO(yingwen): Batch sample ti mestamp in sampling phase.
            lastMemTableForWrite.as_ref().unwrap().put(&mut ctx, keySequence, row, schema, timestamp)
                .context(WriteMemTable { table: &self.tableData.name })?;
        }

        // Update last sequence of memtable.
        for memTableForWrite in memTableForWriteVec {
            memTableForWrite.setLastSequence(sequenceNumber).context(UpdateMemTableSequence)?;
        }

        Ok(())
    }
}

impl<'a> Writer<'a> {
    pub(crate) async fn write(&mut self, writeRequest: WriteRequest) -> Result<usize> {
        let _timer = self.tableData.metrics.start_table_write_execute_timer();
        self.tableData.metrics.on_write_request_begin();

        // 确保1把不能写超过1000万的row
        self.validate_before_write(&writeRequest)?;

        let mut encodeContext = EncodeContext::new(writeRequest.rowGroup);

        self.preprocess_write(&mut encodeContext).await?;

        {
            let _timer = self.tableData.metrics.start_table_write_encode_timer();
            let schema = self.tableData.schema();
            encodeContext.encode_rows(&schema)?;
        }

        let EncodeContext {
            rowGroup,
            indexInWriterSchema,
            encodedRows,
        } = encodeContext;

        let tableData = self.tableData.clone();

        match self.maybeSplitWriteRequest(encodedRows, &rowGroup) {
            SplitResult::Integrate { encodedRows, rowGroupSlicer } => {
                self.writeTableRowGroup(&tableData, rowGroupSlicer, indexInWriterSchema, encodedRows).await?;
            }
            SplitResult::Splitted { encoded_batches, row_group_batches } => {
                for (encodedRows, rowGroupSlicer) in encoded_batches.into_iter().zip(row_group_batches) {
                    self.writeTableRowGroup(&tableData,
                                            rowGroupSlicer,
                                            indexInWriterSchema.clone(),
                                            encodedRows, ).await?;
                }
            }
        }

        Ok(rowGroup.num_rows())
    }

    /// 用来控制要不要分批写 max_bytes_per_write_batch
    fn maybeSplitWriteRequest<'b>(&self, // &'a self
                                  encodedRows: Vec<Vec<u8>>,
                                  rowGroup: &'b RowGroup) -> SplitResult<'b> {
        if self.instance.max_bytes_per_write_batch.is_none() {
            return SplitResult::Integrate {
                encodedRows,
                rowGroupSlicer: RowGroupSlicer::from(rowGroup),
            };
        }

        let splitter = WriteRowGroupSplitter::new(self.instance.max_bytes_per_write_batch.unwrap());
        splitter.split(encodedRows, rowGroup)
    }

    async fn writeTableRowGroup(&mut self,
                                tableData: &Arc<TableData>,
                                rowGroupSlicer: RowGroupSlicer<'_>,
                                indexInWriterSchema: IndexInWriterSchema,
                                encodedRows: Vec<Vec<u8>>) -> Result<()> {
        // 写wal
        let sequenceNumber = self.write2Wal(encodedRows).await?;

        let memTableWriter = MemTableWriter::new(tableData.clone(), self.serial_exec);

        // 写memTable
        memTableWriter.write(sequenceNumber, &rowGroupSlicer, indexInWriterSchema).map_err(|e| {
            error!("failed to write to memtable, table:{}, table_id:{}, err:{}",tableData.name, tableData.id, e);
            e
        })?;

        // failure of writing memtable may cause inconsecutive sequence.
        if tableData.last_sequence() + 1 != sequenceNumber {
            warn!("sequence must be consecutive, table:{}, table_id:{}, last_sequence:{}, wal_sequence:{}",
                tableData.name,tableData.id,tableData.last_sequence(),sequenceNumber);
        }

        debug!("instance write finished, update sequence, table:{}, table_id:{} last_sequence:{}",
            tableData.name, tableData.id, sequenceNumber);

        tableData.set_last_sequence(sequenceNumber);

        tableData.metrics.on_write_request_done(rowGroupSlicer.num_rows());

        Ok(())
    }

    /// Return Ok if the request is valid, this is done before entering the write thread.
    fn validate_before_write(&self, request: &WriteRequest) -> Result<()> {
        ensure!(
            request.rowGroup.num_rows() < MAX_ROWS_TO_WRITE,
            TooManyRows {
                table: &self.tableData.name,
                rows: request.rowGroup.num_rows(),
            }
        );

        Ok(())
    }

    /// Preprocess before write, check:
    ///  - whether table is dropped
    ///  - memtable capacity and maybe trigger flush
    ///
    /// Fills [common_types::schema::IndexInWriterSchema] in [EncodeContext]
    async fn preprocess_write(&mut self, encodeContext: &mut EncodeContext) -> Result<()> {
        let _total_timer = self.tableData.metrics.start_table_write_preprocess_timer();

        ensure!(!self.tableData.is_dropped(), WriteDroppedTable {table: &self.tableData.name,});

        // Checks schema compatibility.
        self.tableData.schema().compatible_for_write(encodeContext.rowGroup.schema(),
                                                     &mut encodeContext.indexInWriterSchema).context(IncompatSchema)?;

        // 默认是false spaces上
        if self.instance.shouldFlushInstance() {
            if let Some(space) = self.instance.spaceStore.find_maximum_memory_usage_space() {
                if let Some(table) = space.find_maximum_memory_usage_table() {
                    info!("Trying to flush table {} bytes {} in space {} because engine total memtable memory usage exceeds db_write_buffer_size {}.",
                          table.name,table.memtable_memory_usage(),space.id,self.instance.db_write_buffer_size,);

                    let _timer = self.tableData.metrics.start_table_write_instance_flush_wait_timer();
                    self.handle_memtable_flush(&table).await?;
                }
            }
        }

        // 默认是false space中的table上
        if self.space.should_flush_space() {
            if let Some(table) = self.space.find_maximum_memory_usage_table() {
                info!("Trying to flush table {} bytes {} in space {} because space total memtable memory usage exceeds space_write_buffer_size {}.",
                      table.name,table.memtable_memory_usage() ,self.space.id,self.space.write_buffer_size,);

                let _timer = self.tableData.metrics.start_table_write_space_flush_wait_timer();
                self.handle_memtable_flush(&table).await?;
            }
        }

        if self.tableData.should_flush_table(self.serial_exec) {
            let table_data = self.tableData.clone();
            let _timer = table_data.metrics.start_table_write_flush_wait_timer();
            self.handle_memtable_flush(&table_data).await?;
        }

        Ok(())
    }

    /// Write log_batch into wal, return the sequence number of log_batch.
    async fn write2Wal(&self, encoded_rows: Vec<ByteVec>) -> Result<SequenceNumber> {
        let _timer = self.tableData.metrics.start_table_write_wal_timer();

        // convert into pb
        let writeRequestPb = table_requests::WriteRequest {
            // FIXME: Shall we avoid the magic number here?
            version: 0,
            // use the table schema instead of the schema in request to avoid schema mismatch during replaying
            schema: Some(schema_pb::TableSchema::from(&self.tableData.schema())),
            rows: encoded_rows,
        };

        // encode payload
        let writePayload = WritePayload::Write(&writeRequestPb);
        let tableLocation = self.tableData.table_location();
        let walLocation = instance::createWalLocation(tableLocation.id, tableLocation.shard_info);
        let logBatchEncoder = LogBatchEncoder::create(walLocation); // walLocation 是key
        let logWriteBatch = logBatchEncoder.encode(&writePayload).context(EncodePayloads {
            table: &self.tableData.name,
            wal_location: walLocation,
        })?;

        // write to wal manager
        let write_ctx = WriteContext::default();
        let sequence = self.instance.spaceStore.walManager.write(&write_ctx, &logWriteBatch).await.context(WriteLogBatch { table: &self.tableData.name })?;

        Ok(sequence)
    }

    /// Flush memtables of table in background.
    ///
    /// Note the table to flush may not the same as `self.table_data`. And if we
    /// try to flush other table in this table's writer, the lock should be
    /// acquired in advance. And in order to avoid deadlock, we should not wait
    /// for the lock.
    async fn handle_memtable_flush(&mut self, table_data: &TableDataRef) -> Result<()> {
        let opts = TableFlushOptions {
            res_sender: None,
            max_retry_flush_limit: self.instance.max_retry_flush_limit(),
        };

        let flusher = self.instance.make_flusher();

        if table_data.id == self.tableData.id {
            let flush_scheduler = self.serial_exec.flush_scheduler();
            // Set `block_on_write_thread` to false and let flush do in background.
            return flusher.scheduleFlush(flush_scheduler, table_data, opts).await.context(FlushTable { table: &table_data.name });
        }

        debug!("try to trigger flush of other table:{} from the write procedure of table:{}",
            table_data.name, self.tableData.name);

        match table_data.tableOpSerialExecutor.try_lock() {
            Ok(mut serial_exec) => {
                let flush_scheduler = serial_exec.flush_scheduler();
                // Set `block_on_write_thread` to false and let flush do in background.
                flusher.scheduleFlush(flush_scheduler, table_data, opts).await.context(FlushTable { table: &table_data.name })
            }
            Err(_) => {
                warn!("Failed to acquire write lock for flush table:{}",table_data.name);
                Ok(())
            }
        }
    }
}