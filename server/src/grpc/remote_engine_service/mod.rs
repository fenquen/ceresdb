// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

// Remote engine rpc service implementation.

use std::{hash::Hash, sync::Arc, time::Instant};

use arrow_ext::ipc::{self, CompressOptions, CompressOutput, CompressionMethod};
use async_trait::async_trait;
use catalog::{manager::ManagerRef, schema::SchemaRef};
use ceresdbproto::{
    remote_engine::{
        read_response::Output::Arrow, remote_engine_service_server::RemoteEngineService,
        GetTableInfoRequest, GetTableInfoResponse, ReadRequest, ReadResponse, WriteBatchRequest,
        WriteRequest, WriteResponse,
    },
    storage::{arrow_payload, ArrowPayload},
};
use common_types::record_batch::RecordBatch;
use futures::stream::{self, BoxStream, FuturesUnordered, StreamExt};
use generic_error::BoxError;
use log::{error, info};
use proxy::instance::InstanceRef;
use query_engine::executor::QueryExecutor as QueryExecutor;
use snafu::{OptionExt, ResultExt};
use table_engine::{
    engine::EngineRuntimes, predicate::PredicateRef, remote::model::TableIdentifier,
    stream::PartitionedStreams, table::TableRef,
};
use time_ext::InstantExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{
    dedup_requests::{RequestNotifiers, RequestResult},
    grpc::{
        metrics::{
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC,
            REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC,
        },
        remote_engine_service::error::{ErrNoCause, ErrWithCause, Result, StatusCode},
    },
};

pub mod error;

const STREAM_QUERY_CHANNEL_LEN: usize = 20;
const DEFAULT_COMPRESS_MIN_LENGTH: usize = 80 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StreamReadReqKey {
    table: String,
    predicate: PredicateRef,
    projection: Option<Vec<usize>>,
}

impl StreamReadReqKey {
    pub fn new(table: String, predicate: PredicateRef, projection: Option<Vec<usize>>) -> Self {
        Self {
            table,
            predicate,
            projection,
        }
    }
}

struct ExecutionGuard<F: FnMut()> {
    f: F,
    cancelled: bool,
}

impl<F: FnMut()> ExecutionGuard<F> {
    fn new(f: F) -> Self {
        Self {
            f,
            cancelled: false,
        }
    }

    fn cancel(&mut self) {
        self.cancelled = true;
    }
}

impl<F: FnMut()> Drop for ExecutionGuard<F> {
    fn drop(&mut self) {
        if !self.cancelled {
            (self.f)()
        }
    }
}

#[derive(Clone)]
pub struct RemoteEngineServiceImpl<Q: QueryExecutor + 'static> {
    pub instance: InstanceRef<Q>,
    pub runtimes: Arc<EngineRuntimes>,
    pub request_notifiers: Option<Arc<RequestNotifiers<StreamReadReqKey, Result<RecordBatch>>>>,
}

impl<Q: QueryExecutor + 'static> RemoteEngineServiceImpl<Q> {
    async fn stream_read_internal(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<ReceiverStream<Result<RecordBatch>>> {
        let instant = Instant::now();
        let ctx = self.handler_ctx();
        let (tx, rx) = mpsc::channel(STREAM_QUERY_CHANNEL_LEN);
        let handle = self.runtimes.read_runtime.spawn(async move {
            let read_request = request.into_inner();
            handle_stream_read(ctx, read_request).await
        });
        let streams = handle.await.box_err().context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to join task",
        })??;

        for stream in streams.streams {
            let mut stream = stream.map(|result| {
                result.box_err().context(ErrWithCause {
                    code: StatusCode::Internal,
                    msg: "record batch failed",
                })
            });
            let tx = tx.clone();
            self.runtimes.read_runtime.spawn(async move {
                let mut num_rows = 0;
                while let Some(batch) = stream.next().await {
                    if let Ok(record_batch) = &batch {
                        num_rows += record_batch.num_rows();
                    }
                    if let Err(e) = tx.send(batch).await {
                        error!("Failed to send handler result, err:{}.", e);
                        break;
                    }
                }
                REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
                    .query_succeeded_row
                    .inc_by(num_rows as u64);
            });
        }

        // TODO(shuangxiao): this metric is invalid, refactor it.
        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .stream_read
            .observe(instant.saturating_elapsed().as_secs_f64());
        Ok(ReceiverStream::new(rx))
    }

    async fn deduped_stream_read_internal(
        &self,
        request_notifiers: Arc<RequestNotifiers<StreamReadReqKey, Result<RecordBatch>>>,
        request: Request<ReadRequest>,
    ) -> Result<ReceiverStream<Result<RecordBatch>>> {
        let instant = Instant::now();
        let ctx = self.handler_ctx();
        let (tx, rx) = mpsc::channel(STREAM_QUERY_CHANNEL_LEN);

        let request = request.into_inner();
        let table_engine::remote::model::ReadRequest {
            table,
            read_request,
        } = request.clone().try_into().box_err().context(ErrWithCause {
            code: StatusCode::BadRequest,
            msg: "fail to convert read request",
        })?;

        let request_key = StreamReadReqKey::new(
            table.table,
            read_request.predicate.clone(),
            read_request.projected_schema.projection(),
        );

        let mut guard = match request_notifiers.insert_notifier(request_key.clone(), tx) {
            // The first request, need to handle it, and then notify the other requests.
            RequestResult::First => {
                // This is used to remove key when future is cancelled.
                ExecutionGuard::new(|| {
                    request_notifiers.take_notifiers(&request_key);
                })
            }
            // The request is waiting for the result of first request.
            RequestResult::Wait => {
                // TODO(shuangxiao): this metric is invalid, refactor it.
                REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
                    .stream_read
                    .observe(instant.saturating_elapsed().as_secs_f64());
                return Ok(ReceiverStream::new(rx));
            }
        };

        let handle = self
            .runtimes
            .read_runtime
            .spawn(async move { handle_stream_read(ctx, request).await });
        let streams = handle.await.box_err().context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to join task",
        })??;

        let mut stream_read = FuturesUnordered::new();
        for stream in streams.streams {
            let mut stream = stream.map(|result| {
                result.box_err().context(ErrWithCause {
                    code: StatusCode::Internal,
                    msg: "record batch failed",
                })
            });

            let handle = self.runtimes.read_runtime.spawn(async move {
                let mut batches = Vec::new();
                while let Some(batch) = stream.next().await {
                    batches.push(batch)
                }

                batches
            });
            stream_read.push(handle);
        }

        let mut batches = Vec::new();
        while let Some(result) = stream_read.next().await {
            let batch = result.box_err().context(ErrWithCause {
                code: StatusCode::Internal,
                msg: "failed to join task",
            })?;
            batches.extend(batch);
        }

        // We should set cancel to guard, otherwise the key will be removed twice.
        guard.cancel();
        let notifiers = request_notifiers.take_notifiers(&request_key).unwrap();

        let num_notifiers = notifiers.len();
        let mut num_rows = 0;
        for batch in batches {
            match batch {
                Ok(batch) => {
                    num_rows += batch.num_rows() * num_notifiers;
                    for notifier in &notifiers {
                        if let Err(e) = notifier.send(Ok(batch.clone())).await {
                            error!("Failed to send handler result, err:{}.", e);
                        }
                    }
                }
                Err(_) => {
                    for notifier in &notifiers {
                        let err = ErrNoCause {
                            code: StatusCode::Internal,
                            msg: "failed to handler request".to_string(),
                        }
                        .fail();
                        if let Err(e) = notifier.send(err).await {
                            error!("Failed to send handler result, err:{}.", e);
                        }
                    }
                    break;
                }
            }
        }

        REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
            .query_succeeded_row
            .inc_by(num_rows as u64);
        REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
            .dedupped_stream_query
            .inc_by((num_notifiers - 1) as u64);
        // TODO(shuangxiao): this metric is invalid, refactor it.
        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .stream_read
            .observe(instant.saturating_elapsed().as_secs_f64());

        Ok(ReceiverStream::new(rx))
    }

    async fn write_internal(
        &self,
        request: Request<WriteRequest>,
    ) -> std::result::Result<Response<WriteResponse>, Status> {
        let begin_instant = Instant::now();
        let ctx = self.handler_ctx();
        let handle = self.runtimes.write_runtime.spawn(async move {
            let request = request.into_inner();
            handle_write(ctx, request).await
        });

        let res = handle.await.box_err().context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to join task",
        });

        let mut resp = WriteResponse::default();
        match res {
            Ok(Ok(v)) => {
                resp.header = Some(error::build_ok_header());
                resp.affected_rows = v.affected_rows;
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(error::build_err_header(e));
            }
        };

        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .write
            .observe(begin_instant.saturating_elapsed().as_secs_f64());
        Ok(Response::new(resp))
    }

    async fn get_table_info_internal(
        &self,
        request: Request<GetTableInfoRequest>,
    ) -> std::result::Result<Response<GetTableInfoResponse>, Status> {
        let begin_instant = Instant::now();
        let ctx = self.handler_ctx();
        let handle = self.runtimes.read_runtime.spawn(async move {
            let request = request.into_inner();
            handle_get_table_info(ctx, request).await
        });

        let res = handle.await.box_err().context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to join task",
        });

        let mut resp = GetTableInfoResponse::default();
        match res {
            Ok(Ok(v)) => {
                resp.header = Some(error::build_ok_header());
                resp.table_info = v.table_info;
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(error::build_err_header(e));
            }
        };

        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .get_table_info
            .observe(begin_instant.saturating_elapsed().as_secs_f64());
        Ok(Response::new(resp))
    }

    async fn write_batch_internal(
        &self,
        request: Request<WriteBatchRequest>,
    ) -> std::result::Result<Response<WriteResponse>, Status> {
        let begin_instant = Instant::now();
        let request = request.into_inner();
        let mut write_table_handles = Vec::with_capacity(request.batch.len());
        for one_request in request.batch {
            let ctx = self.handler_ctx();
            let handle = self
                .runtimes
                .write_runtime
                .spawn(handle_write(ctx, one_request));
            write_table_handles.push(handle);
        }

        let mut batch_resp = WriteResponse {
            header: Some(error::build_ok_header()),
            affected_rows: 0,
        };
        for write_handle in write_table_handles {
            let write_result = write_handle.await.box_err().context(ErrWithCause {
                code: StatusCode::Internal,
                msg: "fail to run the join task",
            });
            // The underlying write can't be cancelled, so just ignore the left write
            // handles (don't abort them) if any error is encountered.
            match write_result {
                Ok(res) => match res {
                    Ok(resp) => batch_resp.affected_rows += resp.affected_rows,
                    Err(e) => {
                        error!("Failed to write batches, err:{e}");
                        batch_resp.header = Some(error::build_err_header(e));
                        break;
                    }
                },
                Err(e) => {
                    error!("Failed to write batches, err:{e}");
                    batch_resp.header = Some(error::build_err_header(e));
                    break;
                }
            };
        }

        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .write_batch
            .observe(begin_instant.saturating_elapsed().as_secs_f64());

        Ok(Response::new(batch_resp))
    }

    fn handler_ctx(&self) -> HandlerContext {
        HandlerContext {
            catalog_manager: self.instance.catalog_manager.clone(),
        }
    }
}

/// Context for handling all kinds of remote engine service.
#[derive(Clone)]
struct HandlerContext {
    catalog_manager: ManagerRef,
}

#[async_trait]
impl<Q: QueryExecutor + 'static> RemoteEngineService for RemoteEngineServiceImpl<Q> {
    type ReadStream = BoxStream<'static, std::result::Result<ReadResponse, Status>>;

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> std::result::Result<Response<Self::ReadStream>, Status> {
        REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC.stream_query.inc();
        let result = match self.request_notifiers.clone() {
            Some(request_notifiers) => {
                self.deduped_stream_read_internal(request_notifiers, request)
                    .await
            }
            None => self.stream_read_internal(request).await,
        };

        match result {
            Ok(stream) => {
                let new_stream: Self::ReadStream = Box::pin(stream.map(|res| match res {
                    Ok(record_batch) => {
                        let resp = match ipc::encode_record_batch(
                            &record_batch.into_arrow_record_batch(),
                            CompressOptions {
                                compress_min_length: DEFAULT_COMPRESS_MIN_LENGTH,
                                method: CompressionMethod::Zstd,
                            },
                        )
                        .box_err()
                        .context(ErrWithCause {
                            code: StatusCode::Internal,
                            msg: "encode record batch failed",
                        }) {
                            Err(e) => ReadResponse {
                                header: Some(error::build_err_header(e)),
                                ..Default::default()
                            },
                            Ok(CompressOutput { payload, method }) => {
                                let compression = match method {
                                    CompressionMethod::None => arrow_payload::Compression::None,
                                    CompressionMethod::Zstd => arrow_payload::Compression::Zstd,
                                };

                                ReadResponse {
                                    header: Some(error::build_ok_header()),
                                    output: Some(Arrow(ArrowPayload {
                                        record_batches: vec![payload],
                                        compression: compression as i32,
                                    })),
                                }
                            }
                        };

                        Ok(resp)
                    }
                    Err(e) => {
                        let resp = ReadResponse {
                            header: Some(error::build_err_header(e)),
                            ..Default::default()
                        };
                        Ok(resp)
                    }
                }));

                Ok(Response::new(new_stream))
            }
            Err(e) => {
                let resp = ReadResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                };
                let stream = stream::once(async { Ok(resp) });
                Ok(Response::new(Box::pin(stream)))
            }
        }
    }

    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> std::result::Result<Response<WriteResponse>, Status> {
        self.write_internal(request).await
    }

    async fn get_table_info(
        &self,
        request: Request<GetTableInfoRequest>,
    ) -> std::result::Result<Response<GetTableInfoResponse>, Status> {
        self.get_table_info_internal(request).await
    }

    async fn write_batch(
        &self,
        request: Request<WriteBatchRequest>,
    ) -> std::result::Result<Response<WriteResponse>, Status> {
        self.write_batch_internal(request).await
    }
}

async fn handle_stream_read(
    ctx: HandlerContext,
    request: ReadRequest,
) -> Result<PartitionedStreams> {
    let table_engine::remote::model::ReadRequest {
        table: table_ident,
        read_request,
    } = request.try_into().box_err().context(ErrWithCause {
        code: StatusCode::BadRequest,
        msg: "fail to convert read request",
    })?;

    let request_id = read_request.request_id;
    info!(
        "Handle stream read, request_id:{request_id}, table:{table_ident:?}, read_options:{:?}, predicate:{:?} ",
        read_request.readOptions,
        read_request.predicate,
    );

    let begin = Instant::now();
    let table = find_table_by_identifier(&ctx, &table_ident)?;
    let res = table
        .partitionedRead(read_request)
        .await
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to read table, table:{table_ident:?}"),
        });
    match res {
        Ok(streams) => {
            info!(
        "Handle stream read success, request_id:{request_id}, table:{table_ident:?}, cost:{:?}",
        begin.elapsed(),
    );
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
                .stream_query_succeeded
                .inc();
            Ok(streams)
        }
        Err(e) => {
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
                .stream_query_failed
                .inc();
            Err(e)
        }
    }
}

async fn handle_write(ctx: HandlerContext, request: WriteRequest) -> Result<WriteResponse> {
    let write_request: table_engine::remote::model::WriteRequest =
        request.try_into().box_err().context(ErrWithCause {
            code: StatusCode::BadRequest,
            msg: "fail to convert write request",
        })?;

    let num_rows = write_request.write_request.rowGroup.num_rows();
    let table = find_table_by_identifier(&ctx, &write_request.table)?;

    let res = table
        .write(write_request.write_request)
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to write table, table:{:?}", write_request.table),
        });
    match res {
        Ok(affected_rows) => {
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC.write_succeeded.inc();
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
                .write_succeeded_row
                .inc_by(affected_rows as u64);
            Ok(WriteResponse {
                header: None,
                affected_rows: affected_rows as u64,
            })
        }
        Err(e) => {
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC.write_failed.inc();
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
                .write_failed_row
                .inc_by(num_rows as u64);
            Err(e)
        }
    }
}

async fn handle_get_table_info(
    ctx: HandlerContext,
    request: GetTableInfoRequest,
) -> Result<GetTableInfoResponse> {
    let request: table_engine::remote::model::GetTableInfoRequest =
        request.try_into().box_err().context(ErrWithCause {
            code: StatusCode::BadRequest,
            msg: "fail to convert get table info request",
        })?;

    let schema = find_schema_by_identifier(&ctx, &request.table)?;
    let table = schema
        .table_by_name(&request.table.table)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to get table, table:{}", request.table.table),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!("table is not found, table:{}", request.table.table),
        })?;

    Ok(GetTableInfoResponse {
        header: None,
        table_info: Some(ceresdbproto::remote_engine::TableInfo {
            catalog_name: request.table.catalog,
            schema_name: schema.name().to_string(),
            schema_id: schema.id().as_u32(),
            table_name: table.name().to_string(),
            table_id: table.id().as_u64(),
            table_schema: Some((&table.schema()).into()),
            engine: table.engine_type().to_string(),
            options: table.options(),
            partition_info: table.partition_info().map(Into::into),
        }),
    })
}

fn find_table_by_identifier(
    ctx: &HandlerContext,
    table_identifier: &TableIdentifier,
) -> Result<TableRef> {
    let schema = find_schema_by_identifier(ctx, table_identifier)?;

    schema
        .table_by_name(&table_identifier.table)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to get table, table:{}", table_identifier.table),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!("table is not found, table:{}", table_identifier.table),
        })
}

fn find_schema_by_identifier(
    ctx: &HandlerContext,
    table_identifier: &TableIdentifier,
) -> Result<SchemaRef> {
    let catalog = ctx
        .catalog_manager
        .catalog_by_name(&table_identifier.catalog)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to get catalog, catalog:{}", table_identifier.catalog),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!("catalog is not found, catalog:{}", table_identifier.catalog),
        })?;
    catalog
        .schema_by_name(&table_identifier.schema)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!(
                "fail to get schema of table, schema:{}",
                table_identifier.schema
            ),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!(
                "schema of table is not found, schema:{}",
                table_identifier.schema
            ),
        })
}
