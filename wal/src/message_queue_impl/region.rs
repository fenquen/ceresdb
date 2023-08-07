// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Region in wal on message queue

use std::{cmp, sync::Arc};

use common_types::{table::TableId, SequenceNumber};
use generic_error::{BoxError, GenericError};
use log::{debug, info};
use macros::define_result;
use message_queue::{ConsumeIterator, MessageQueue, Offset, OffsetType, StartOffset};
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use tokio::sync::{Mutex, RwLock};
use util::*;

use crate::{
    kv_encoder::CommonLogEncoding,
    log_batch::{LogEntry, LogWriteBatch},
    manager,
    message_queue_impl::{
        encoding::{format_wal_data_topic_name, format_wal_meta_topic_name, MetaEncoding},
        log_cleaner::LogCleaner,
        region_context::{
            self, RegionContext, RegionContextBuilder, RegionMetaDelta, RegionMetaSnapshot,
            TableMetaData, TableWriteContext,
        },
        snapshot_synchronizer::{self, SnapshotSynchronizer},
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Write logs to region failed, err:{}", source))]
    Write { source: region_context::Error },

    #[snafu(display(
        "Failed to scan logs from region, region id:{}, msg:{}\nBacktrace:{}",
        region_id,
        msg,
        backtrace
    ))]
    ScanNoCause {
        region_id: u64,
        table_id: Option<TableId>,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to scan logs from region with cause, region id:{}, table id:{:?}, msg:{:?}, err:{}",
        region_id,
        table_id,
        msg,
        source
    ))]
    ScanWithCause {
        region_id: u64,
        table_id: Option<TableId>,
        msg: String,
        source: GenericError,
    },

    #[snafu(display("Failed to get table meta data, err:{}", source))]
    GetTableMeta { source: region_context::Error },

    #[snafu(display("Failed to mark deleted sequence to table, err:{}", source))]
    MarkDeleteTo { source: region_context::Error },

    #[snafu(display("Failed to sync snapshot of region, err:{}", source))]
    SyncSnapshot {
        source: snapshot_synchronizer::Error,
    },

    #[snafu(display("Failed to clean logs of region, err:{}", source))]
    CleanLogs { source: GenericError },

    #[snafu(display(
        "Failed to open region with cause, namespace:{}, region id:{}, msg:{}, err:{}",
        namespace,
        region_id,
        msg,
        source
    ))]
    OpenWithCause {
        namespace: String,
        region_id: u64,
        msg: String,
        source: GenericError,
    },

    #[snafu(display(
        "Failed to open region with no cause, namespace:{}, region id:{}, msg:{}, \nBacktrace:\n{}",
        namespace,
        region_id,
        msg,
        backtrace
    ))]
    OpenNoCause {
        namespace: String,
        region_id: u64,
        msg: String,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// Region in wal(message queue based)
pub struct Region<M: MessageQueue> {
    /// Region inner, see [RegionInner]
    ///
    /// Most of time, lock by `read lock`.
    /// While needing to freeze the region(such as, make a snapshot),
    /// `write lock` will be used.
    inner: RwLock<RegionInner<M>>,

    /// Will synchronize the snapshot to message queue by it
    ///
    /// Lock for forcing the snapshots to be synchronized sequentially.
    snapshot_synchronizer: Mutex<SnapshotSynchronizer<M>>,

    /// Clean the outdated logs which are marked delete
    log_cleaner: Mutex<LogCleaner<M>>,
}

impl<M: MessageQueue> Region<M> {
    /// Init the region.
    pub async fn open(namespace: &str, region_id: u64, message_queue: Arc<M>) -> Result<Self> {
        info!(
            "Begin to open region in namespace, namespace:{}, region id:{}",
            namespace, region_id
        );

        // Format to the topic name.
        let log_topic = format_wal_data_topic_name(namespace, region_id);
        let meta_topic = format_wal_meta_topic_name(namespace, region_id);
        let log_encoding = CommonLogEncoding::newest();
        let meta_encoding = MetaEncoding::newest();

        message_queue
            .create_topic_if_not_exist(&log_topic)
            .await
            .box_err()
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while trying to create topic",
            })?;

        message_queue
            .create_topic_if_not_exist(&meta_topic)
            .await
            .box_err()
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while trying to create topic",
            })?;

        // Build region meta.
        let mut region_meta_builder = RegionContextBuilder::new(region_id);
        let region_safe_delete_offset = Self::recover_region_meta_from_meta(
            namespace,
            region_id,
            message_queue.as_ref(),
            &meta_topic,
            &meta_encoding,
            &mut region_meta_builder,
        )
        .await?;

        Self::recover_region_meta_from_log(
            namespace,
            region_id,
            message_queue.as_ref(),
            region_safe_delete_offset,
            &log_topic,
            &log_encoding,
            &mut region_meta_builder,
        )
        .await?;

        // Init region inner.
        let inner = RwLock::new(RegionInner::new(
            region_meta_builder.build(),
            log_encoding,
            message_queue.clone(),
            log_topic.clone(),
        ));

        // Init others.
        let snapshot_synchronizer = Mutex::new(SnapshotSynchronizer::new(
            region_id,
            message_queue.clone(),
            meta_topic,
            meta_encoding,
        ));
        let log_cleaner = Mutex::new(LogCleaner::new(region_id, message_queue.clone(), log_topic));

        info!(
            "Finish opening region in namespace, namespace:{}, region id:{}",
            namespace, region_id
        );

        Ok(Region {
            inner,
            snapshot_synchronizer,
            log_cleaner,
        })
    }

    async fn recover_region_meta_from_meta(
        namespace: &str,
        region_id: u64,
        message_queue: &M,
        meta_topic: &str,
        meta_encoding: &MetaEncoding,
        builder: &mut RegionContextBuilder,
    ) -> Result<Option<Offset>> {
        info!(
            "Recover region meta from meta, namespace:{}, region id:{}",
            namespace, region_id
        );

        // Fetch earliest, high watermark and check.
        let earliest = message_queue
            .fetch_offset(meta_topic, OffsetType::EarliestOffset)
            .await
            .box_err()
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while recover from meta",
            })?;

        let high_watermark = message_queue
            .fetch_offset(meta_topic, OffsetType::HighWaterMark)
            .await
            .box_err()
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while recover from meta",
            })?;

        if earliest == high_watermark {
            if high_watermark == 0 {
                info!("Recover region meta from meta, found empty meta topic, just need to recover from log topic, namespace:{}, region id:{}",
                    namespace, region_id);
                return Ok(None);
            }

            return OpenNoCause {
                namespace,
                region_id,
                msg: "region meta impossible to be empty when having written logs",
            }
            .fail();
        }

        // Fetch snapshot from meta topic(just fetch the last snapshot).
        let mut iter = message_queue
            .consume(meta_topic, StartOffset::At(high_watermark - 1))
            .await
            .box_err()
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while recover from meta",
            })?;

        let (latest_message_and_offset, returned_high_watermark) =
            iter.next_message().await.box_err().context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while recover from meta",
            })?;

        ensure!(returned_high_watermark == high_watermark, OpenNoCause { namespace , region_id, msg: format!(
            "failed while recover from meta, high watermark shouldn't changed while opening region,
            origin high watermark:{high_watermark}, returned high watermark:{returned_high_watermark}")
        });

        // Decode and apply it to builder.
        let raw_key = latest_message_and_offset
            .message
            .key
            .with_context(|| OpenNoCause {
                namespace,
                region_id,
                msg: "failed while recover from meta, key in message shouldn't be None",
            })?;

        let raw_value = latest_message_and_offset
            .message
            .value
            .with_context(|| OpenNoCause {
                namespace,
                region_id,
                msg: "failed while recover from meta, value in message shouldn't be None",
            })?;

        let key = meta_encoding
            .decode_key(raw_key.as_slice())
            .box_err()
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while recover from meta",
            })?;

        ensure!(key.0 == region_id, OpenNoCause { namespace , region_id, msg: format!(
            "failed while recover from meta, region id in key should be equal to the one of current region,
            but now are {} and {}", key.0, region_id)
        });

        let value = meta_encoding
            .decode_value(raw_value.as_slice())
            .box_err()
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while recover from meta",
            })?;

        let min_safe_delete_offset = value.entries.iter().fold(i64::MAX, |min_offset, entry| {
            match entry.safe_delete_offset {
                Some(offset) => cmp::min(min_offset, offset),
                None => min_offset,
            }
        });

        let region_safe_delete_offset = if min_safe_delete_offset == i64::MAX {
            info!("Recover region meta from meta, min_safe_delete_offset not exist, region_meta_snapshot:{:?}, namespace:{}, region id:{}",
                value, namespace, region_id);

            None
        } else {
            Some(min_safe_delete_offset)
        };

        builder
            .apply_region_meta_snapshot(value)
            .box_err()
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while recover from meta",
            })?;

        Ok(region_safe_delete_offset)
    }

    async fn recover_region_meta_from_log(
        namespace: &str,
        region_id: u64,
        message_queue: &M,
        region_safe_delete_offset: Option<Offset>,
        log_topic: &str,
        log_encoding: &CommonLogEncoding,
        builder: &mut RegionContextBuilder,
    ) -> Result<()> {
        info!(
            "Recover region meta from log, namespace:{namespace}, region_id:{region_id}, region_safe_delete_offset:{region_safe_delete_offset:?}",
        );

        let start_offset = match region_safe_delete_offset {
            Some(offset) => StartOffset::At(offset),
            None => StartOffset::Earliest,
        };

        // Fetch snapshot from meta topic(just fetch the last snapshot).
        // FIXME: should not judge whether topic is empty or not by caller.
        // The consumer iterator should return immediately rather than hanging when
        // topic empty.
        // Fetch earliest, high watermark and check.
        let earliest = message_queue
            .fetch_offset(log_topic, OffsetType::EarliestOffset)
            .await
            .box_err()
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while recover from log",
            })?;

        let high_watermark = message_queue
            .fetch_offset(log_topic, OffsetType::HighWaterMark)
            .await
            .box_err()
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while recover from log",
            })?;

        if earliest == high_watermark {
            info!("Recover region meta from log, found empty log topic, namespace:{}, region_id:{}, earliest:{}, high_watermark:{}",
                namespace, region_id, earliest, high_watermark
            );
            return Ok(());
        }

        let mut iter = message_queue
            .consume(log_topic, start_offset)
            .await
            .box_err()
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while recover from log",
            })?;

        loop {
            let (latest_message_and_offset, returned_high_watermark) =
                iter.next_message().await.box_err().context(OpenWithCause {
                    namespace,
                    region_id,
                    msg: "failed while recover from log",
                })?;

            ensure!(returned_high_watermark == high_watermark, OpenNoCause { namespace , region_id, msg: format!(
                "failed while recover from log, high watermark shouldn't changed while opening region,
                origin high watermark:{high_watermark}, returned high watermark:{returned_high_watermark}")
            });

            // Decode and apply it to builder.
            let raw_key = latest_message_and_offset
                .message
                .key
                .with_context(|| OpenNoCause {
                    namespace,
                    region_id,
                    msg: "failed while recover from log, key in message shouldn't be None",
                })?;

            let key = log_encoding
                .decode_key(raw_key.as_slice())
                .box_err()
                .context(OpenWithCause {
                    namespace,
                    region_id,
                    msg: "failed while recover from log",
                })?;

            ensure!(key.region_id == region_id, OpenNoCause { namespace , region_id, msg: format!(
                "failed while recover from log, region id in key should be equal to the one of current region,
                but now are {} and {}", key.region_id, region_id)
            });

            // TODO: maybe this clone should be avoided?
            let region_meta_delta = RegionMetaDelta::new(
                key.table_id,
                key.sequence_num,
                latest_message_and_offset.offset,
            );

            builder
                .apply_region_meta_delta(region_meta_delta.clone())
                .box_err()
                .context(OpenWithCause {
                    namespace,
                    region_id,
                    msg: format!(
                        "failed while recover from log, region meta delta:{region_meta_delta:?}"
                    ),
                })?;

            // Has polled last log in topic, break.
            if latest_message_and_offset.offset + 1 == high_watermark {
                debug!("Has polled last log from topic, break the loop");
                break;
            }
        }

        Ok(())
    }

    /// Write logs of table to region.
    pub async fn write(
        &self,
        ctx: &manager::WriteContext,
        log_batch: &LogWriteBatch,
    ) -> Result<SequenceNumber> {
        let inner = self.inner.read().await;

        debug!(
            "Begin to write to wal region, ctx:{:?}, region id:{}, location:{:?}, log_entries_num:{}",
            ctx,
            inner.region_context.region_id(),
            log_batch.walLocation,
            log_batch.logWriteEntryVec.len()
        );

        inner.write(ctx, log_batch).await
    }

    /// Scan all logs from region.
    ///
    /// NOTICE: we get scan range from the region's snapshot, if call
    /// `mark_delete_to` during polling logs concurrently, it may lead to
    /// error.
    pub async fn scan_region(
        &self,
        ctx: &manager::ReadContext,
    ) -> Result<Option<MessageQueueLogIterator<M::ConsumeIterator>>> {
        // Calculate region's scan range from its snapshot.
        let scan_range = {
            let inner = self.inner.write().await;

            info!(
                "Prepare to scan all logs from region, region id:{}, log topic:{}, ctx:{:?}",
                inner.region_context.region_id(),
                inner.log_topic,
                ctx
            );

            let snapshot = inner.make_meta_snapshot().await;
            let mut safe_delete_offset = Offset::MAX;
            let mut high_watermark = 0;
            // Calculate the min offset in message queue.
            for table_meta in &snapshot.entries {
                if let Some(offset) = table_meta.safe_delete_offset {
                    safe_delete_offset = cmp::min(safe_delete_offset, offset);
                }
                high_watermark = cmp::max(high_watermark, table_meta.current_high_watermark);
            }

            if safe_delete_offset == Offset::MAX {
                None
            } else {
                assert!(safe_delete_offset < high_watermark);
                Some(ScanRange::new(safe_delete_offset, high_watermark))
            }
        };

        match scan_range {
            Some(scan_range) => {
                let inner = self.inner.read().await;
                Ok(Some(
                    inner
                        .range_scan(ctx, None, scan_range)
                        .await
                        .context(ScanWithCause {
                            region_id: inner.region_context.region_id(),
                            table_id: None,
                            msg: format!(
                                "failed while creating iterator, scan range:{scan_range:?}"
                            ),
                        })?,
                ))
            }

            None => Ok(None),
        }
    }

    /// Scan logs of specific table from region.
    ///
    /// NOTICE: we get scan range from the table's snapshot, if call
    /// `mark_delete_to` for the same table during polling logs concurrently, it
    /// may lead to error.
    pub async fn scan_table(
        &self,
        table_id: TableId,
        ctx: &manager::ReadContext,
    ) -> Result<Option<MessageQueueLogIterator<M::ConsumeIterator>>> {
        let (table_id, scan_range) = {
            let inner = self.inner.read().await;

            debug!(
                "Prepare to scan logs of the table from region, region id:{}, table id:{}, log topic:{}, ctx:{:?}",
                inner.region_context.region_id(), table_id, inner.log_topic, ctx
            );

            let table_meta = match inner.get_table_meta(table_id).await? {
                Some(table_meta) => table_meta,
                None => {
                    return Ok(None);
                }
            };

            if let Some(start_offset) = table_meta.safe_delete_offset {
                (
                    table_id,
                    Some(ScanRange::new(
                        start_offset,
                        table_meta.current_high_watermark,
                    )),
                )
            } else {
                (table_id, None)
            }
        };

        match scan_range {
            Some(scan_range) => {
                let inner = self.inner.read().await;
                Ok(Some(
                    inner
                        .range_scan(ctx, Some(table_id), scan_range)
                        .await
                        .context(ScanWithCause {
                            region_id: inner.region_context.region_id(),
                            table_id: Some(table_id),
                            msg: format!(
                                "failed while creating iterator, scan range:{scan_range:?}"
                            ),
                        })?,
                ))
            }

            None => Ok(None),
        }
    }

    /// Mark the entries whose sequence number is in [0, `next sequence number`)
    /// to be deleted in the future.
    pub async fn mark_delete_to(
        &self,
        table_id: TableId,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        let (snapshot, synchronizer) = {
            let inner = self.inner.write().await;

            info!(
                "Mark deleted entries to sequence num:{}, region id:{}, table id:{}",
                sequence_num,
                inner.region_context.region_id(),
                table_id
            );

            inner.mark_delete_to(table_id, sequence_num).await?;

            (
                inner.make_meta_snapshot().await,
                self.snapshot_synchronizer.lock().await,
            )
        };

        // TODO: a temporary and rough implementation...
        // just need to sync the snapshot while dropping table, but now we sync while
        // every flushing... Just sync here now, obviously it is not enough.
        synchronizer.sync(snapshot).await.context(SyncSnapshot)
    }

    /// Get meta data by table id.
    pub async fn get_table_meta(&self, table_id: TableId) -> Result<Option<TableMetaData>> {
        let inner = self.inner.read().await;
        inner.get_table_meta(table_id).await
    }

    /// Clean outdated logs according to the information in region snapshot.
    pub async fn clean_logs(&self) -> Result<()> {
        // Get current snapshot.
        let (snapshot, synchronizer) = {
            let inner = self.inner.write().await;
            (
                inner.make_meta_snapshot().await,
                self.snapshot_synchronizer.lock().await,
            )
        };

        let safe_delete_offset = snapshot.safe_delete_offset();
        info!("Region clean logs, snapshot:{snapshot:?}, safe_delete_offset:{safe_delete_offset}");

        // Sync snapshot first.
        synchronizer
            .sync(snapshot)
            .await
            .box_err()
            .context(CleanLogs)?;

        // Check and maybe clean logs then.
        let mut log_cleaner = self.log_cleaner.lock().await;
        log_cleaner
            .maybe_clean_logs(safe_delete_offset)
            .await
            .box_err()
            .context(CleanLogs)
    }

    /// Return snapshot, just used for test.
    pub async fn make_meta_snapshot(&self) -> RegionMetaSnapshot {
        let inner = self.inner.write().await;
        inner.make_meta_snapshot().await
    }
}

/// Region's inner, all methods of [Region] are mainly implemented in it.
struct RegionInner<M> {
    /// Region meta data(such as, tables' next sequence numbers)
    region_context: RegionContext,

    /// Used to encode/decode the logs
    log_encoding: CommonLogEncoding,

    /// Message queue's Client
    message_queue: Arc<M>,

    /// Topic storing logs in message queue
    log_topic: String,
}

impl<M: MessageQueue> RegionInner<M> {
    pub fn new(
        region_context: RegionContext,
        log_encoding: CommonLogEncoding,
        message_queue: Arc<M>,
        log_topic: String,
    ) -> Self {
        Self {
            region_context,
            log_encoding,
            message_queue,
            log_topic,
        }
    }

    async fn write(
        &self,
        ctx: &manager::WriteContext,
        log_batch: &LogWriteBatch,
    ) -> Result<SequenceNumber> {
        let table_write_ctx = TableWriteContext {
            log_encoding: self.log_encoding.clone(),
            log_topic: self.log_topic.clone(),
            message_queue: self.message_queue.clone(),
        };

        self.region_context
            .write_table_logs(ctx, log_batch, &table_write_ctx)
            .await
            .context(Write)
    }

    // TODO: take each read's timeout in consideration.
    async fn range_scan(
        &self,
        _ctx: &manager::ReadContext,
        table_id: Option<TableId>,
        scan_range: ScanRange,
    ) -> std::result::Result<MessageQueueLogIterator<M::ConsumeIterator>, GenericError> {
        let consume_iter = self
            .message_queue
            .consume(&self.log_topic, StartOffset::At(scan_range.inclusive_start))
            .await
            .map_err(Box::new)?;

        debug!("Create scanning iterator successfully, region id:{}, table id:{:?}, log topic:{}, scan range{:?}", self.region_context.region_id(),
            table_id, self.log_topic, scan_range);

        Ok(MessageQueueLogIterator::new(
            self.region_context.region_id(),
            table_id,
            Some(scan_range.exclusive_end),
            consume_iter,
            self.log_encoding.clone(),
        ))
    }

    async fn mark_delete_to(&self, table_id: TableId, sequence_num: SequenceNumber) -> Result<()> {
        self.region_context
            .mark_table_delete_to(table_id, sequence_num)
            .await
            .context(MarkDeleteTo)
    }

    async fn get_table_meta(&self, table_id: TableId) -> Result<Option<TableMetaData>> {
        self.region_context
            .get_table_meta_data(table_id)
            .await
            .context(GetTableMeta)
    }

    /// Get meta data snapshot of whole region.
    ///
    /// NOTICE: should freeze whole region before calling.
    async fn make_meta_snapshot(&self) -> RegionMetaSnapshot {
        self.region_context.make_snapshot().await
    }
}

// TODO: define some high-level iterator based on this,
// such as `RegionScanIterator` placing the high watermark invariant checking
// in it.
#[derive(Debug)]
pub struct MessageQueueLogIterator<C: ConsumeIterator> {
    /// Id of region
    region_id: u64,

    /// Id of table id
    ///
    /// It will be `None` while scanning region,
    /// and will be `Some` while scanning table.
    table_id: Option<TableId>,

    /// Polling's end point
    ///
    /// While fetching in slave node, it will be set to `None`, and
    /// reading will not stop.
    /// Otherwise, it will be set to high watermark.
    terminate_offset: Option<Offset>,

    /// Terminated flag
    is_terminated: bool,

    /// Consume Iterator of message queue
    iter: C,

    /// Used to encode/decode the logs
    log_encoding: CommonLogEncoding,

    /// See the same problem in https://github.com/CeresDB/ceresdb/issues/120
    previous_value: Vec<u8>,
    // TODO: timeout
}

impl<C: ConsumeIterator> MessageQueueLogIterator<C> {
    fn new(
        region_id: u64,
        table_id: Option<TableId>,
        terminate_offset: Option<Offset>,
        iter: C,
        log_encoding: CommonLogEncoding,
    ) -> Self {
        Self {
            region_id,
            table_id,
            terminate_offset,
            iter,
            is_terminated: false,
            log_encoding,
            previous_value: Vec::new(),
        }
    }
}

impl<C: ConsumeIterator> MessageQueueLogIterator<C> {
    pub async fn next_log_entry(&mut self) -> Result<Option<LogEntry<&'_ [u8]>>> {
        if self.is_terminated && self.terminate_offset.is_some() {
            debug!(
                "Finished to poll all logs from message queue, region id:{}, terminate offset:{:?}",
                self.region_id, self.terminate_offset
            );
            return Ok(None);
        }

        let (message_and_offset, high_watermark) = self
            .iter
            .next_message()
            .await
            .box_err()
            .context(ScanWithCause {
                region_id: self.region_id,
                table_id: self.table_id,
                msg: "failed while polling log",
            })?;

        if let Some(terminate_offset) = &self.terminate_offset {
            ensure!(*terminate_offset <= high_watermark, ScanNoCause {
                region_id: self.region_id,
                table_id: self.table_id,
                msg: format!("the setting terminate offset is invalid, it should be less than or equals to high watermark, terminate offset:{terminate_offset}, high watermark:{high_watermark}"),
            });

            if message_and_offset.offset + 1 == *terminate_offset {
                self.is_terminated = true;
            }
        }

        // Decode the message to log key and value, then create the returned log entry.
        // Key and value in message should absolutely exist.
        let log_key = self
            .log_encoding
            .decode_key(&message_and_offset.message.key.unwrap())
            .box_err()
            .context(ScanWithCause {
                region_id: self.region_id,
                table_id: self.table_id,
                msg: "failed while polling log",
            })?;

        ensure!(
            log_key.region_id == self.region_id,
            ScanNoCause {
                region_id: self.region_id,
                table_id: self.table_id,
                msg: format!(
                    "invalid region id in message, real:{}, expected:{}",
                    self.region_id, log_key.region_id
                ),
            }
        );

        let log_value = message_and_offset.message.value.unwrap();
        let payload = self
            .log_encoding
            .decode_value(&log_value)
            .box_err()
            .context(ScanWithCause {
                region_id: self.region_id,
                table_id: self.table_id,
                msg: "failed while polling log",
            })?;

        self.previous_value = payload.to_owned();

        Ok(Some(LogEntry {
            table_id: log_key.table_id,
            sequence: log_key.sequence_num,
            payload: self.previous_value.as_slice(),
        }))
    }
}

mod util {
    use message_queue::Offset;

    #[derive(Debug, Default, Clone, Copy)]
    pub struct ScanRange {
        pub inclusive_start: Offset,
        pub exclusive_end: Offset,
    }

    impl ScanRange {
        pub fn new(inclusive_start: Offset, exclusive_end: Offset) -> Self {
            Self {
                inclusive_start,
                exclusive_end,
            }
        }
    }
}