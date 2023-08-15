// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Contains common methods used by the read process.

use std::time::Instant;

use ceresdbproto::storage::{
    storage_service_client::StorageServiceClient, RequestContext, SqlQueryRequest, SqlQueryResponse,
};
use common_types::request_id::RequestId;
use futures::FutureExt;
use generic_error::BoxError;
use http::StatusCode;
use interpreters::interpreter::Output;
use log::{error, info, warn};
use query_engine::executor::QueryExecutor as QueryExecutor;
use query_frontend::{
    frontend,
    frontend::{Context as SqlContext, Frontend},
    provider::CatalogMetaProvider,
};
use router::endpoint::Endpoint;
use snafu::{ensure, ResultExt};
use time_ext::InstantExt;
use tonic::{transport::Channel, IntoRequest};

use crate::{
    error::{ErrNoCause, ErrWithCause, Error, Internal, Result},
    forward::{ForwardRequest, ForwardResult},
    Context, Proxy,
};

pub enum SqlResponse {
    Forwarded(SqlQueryResponse),
    Local(Output),
}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub(crate) async fn handle_sql(&self,
                                   ctx: Context,
                                   schema: &str,
                                   sql: &str) -> Result<SqlResponse> {
        if let Some(resp) = self.maybe_forward_sql_query(ctx.clone(), schema, sql).await? {
            match resp {
                ForwardResult::Forwarded(resp) => return Ok(SqlResponse::Forwarded(resp?)),
                ForwardResult::Local => (),
            }
        };

        Ok(SqlResponse::Local(self.fetch_sql_query_output(ctx, schema, sql).await?))
    }

    pub(crate) async fn fetch_sql_query_output(&self,
                                               ctx: Context,
                                               schemaName: &str,
                                               sql: &str) -> Result<Output> {
        let request_id = RequestId::next_id();
        let beginTime = Instant::now();
        let deadline = ctx.timeout.map(|t| beginTime + t);
        let catalogName = self.instance.catalog_manager.default_catalog_name();

        info!("Handle sql query, request_id:{}, schema:{}, sql:{}", request_id, schemaName, sql);

        let instance = &self.instance;
        // TODO(yingwen): Privilege check, cannot access data of other tenant
        // TODO(yingwen): Maybe move MetaProvider to instance
        let provider = CatalogMetaProvider {
            manager: instance.catalog_manager.clone(),
            default_catalog: catalogName,
            default_schema: schemaName,
            function_registry: &*instance.function_registry,
        };
        let frontend = Frontend::new(provider);

        let mut sqlContext = SqlContext::new(request_id, deadline);

        // Parse sql, frontend error of invalid sql already contains sql
        // TODO(yingwen): Maybe move sql from frontend error to outer error
        let mut statementVec = frontend
            .parse_sql(&mut sqlContext, sql)
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::BAD_REQUEST,
                msg: "Failed to parse sql",
            })?;

        ensure!(!statementVec.is_empty(),
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("No valid query statement provided, sql:{sql}",),
            }
        );

        // TODO(yingwen): For simplicity, we only support executing one statement now
        // TODO(yingwen): INSERT/UPDATE/DELETE can be batched
        ensure!(
            statementVec.len() == 1,
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("only support execute one statement, current num:{}, sql:{}",statementVec.len(),sql),
            }
        );

        // Open partition table if needed.
        let table_name = frontend::parse_table_name(&statementVec);
        if let Some(table_name) = &table_name {
            self.maybe_open_partition_table_if_not_exist(catalogName, schemaName, table_name).await?;
        }

        // Create logical plan
        // Note: Remember to store sql in error when creating logical plan
        let plan = frontend
            // TODO(yingwen): Check error, some error may indicate that the sql is invalid. now return internal server error in those cases
            .statementToPlan(&mut sqlContext, statementVec.remove(0))
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to create plan, query:{sql}"),
            })?;

        let mut plan_maybe_expired = false;
        if let Some(table_name) = &table_name {
            match self.is_plan_expired(&plan, catalogName, schemaName, table_name) {
                Ok(v) => plan_maybe_expired = v,
                Err(err) => {
                    warn!("Plan expire check failed, err:{err}");
                }
            }
        }

        let output = self.execute_plan(request_id, catalogName, schemaName, plan, deadline, ctx.enable_partition_table_access).await;

        let output = output.box_err().with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!("Failed to execute plan, sql:{sql}"),
        })?;

        let cost = beginTime.saturating_elapsed();
        info!("handle sql query success, catalog:{catalogName}, schema:{schemaName}, request_id:{request_id}, cost:{cost:?}, sql:{sql:?}");

        match &output {
            Output::AffectedRows(_) => Ok(output),
            Output::Records(v) => {
                if plan_maybe_expired {
                    let row_nums = v.iter().fold(0_usize, |acc, record_batch| acc + record_batch.num_rows());
                    if row_nums == 0 {
                        warn!("query time range maybe exceed TTL, sql:{sql}");

                        // TODO: Cannot return this error directly, empty query
                        // should return 200, not 4xx/5xx
                        // All protocols should recognize this error.
                        // return Err(Error::QueryMaybeExceedTTL {
                        //     msg: format!("Query time range maybe exceed TTL,
                        // sql:{sql}"), });
                    }
                }
                Ok(output)
            }
        }
    }

    async fn maybe_forward_sql_query(&self,
                                     ctx: Context,
                                     schema: &str,
                                     sql: &str) -> Result<Option<ForwardResult<SqlQueryResponse, Error>>> {
        let table_name = frontend::parse_table_name_with_sql(sql)
            .box_err()
            .with_context(|| Internal { msg: format!("Failed to parse table name with sql, sql:{sql}") })?;
        if table_name.is_none() {
            warn!("Unable to forward sql query without table name, sql:{sql}",);
            return Ok(None);
        }

        let sql_request = SqlQueryRequest {
            context: Some(RequestContext {
                database: schema.to_string(),
            }),
            tables: vec![],
            sql: sql.to_string(),
        };

        let forward_req = ForwardRequest {
            schema: schema.to_string(),
            table: table_name.unwrap(),
            req: sql_request.into_request(),
            forwarded_from: ctx.forwarded_from,
        };
        let do_query =
            |mut client: StorageServiceClient<Channel>, request: tonic::Request<SqlQueryRequest>, _: &Endpoint| {
                let query = async move {
                    client
                        .sql_query(request)
                        .await
                        .map(|resp| resp.into_inner())
                        .box_err()
                        .context(ErrWithCause {
                            code: StatusCode::INTERNAL_SERVER_ERROR,
                            msg: "Forwarded sql query failed",
                        })
                }
                    .boxed();

                Box::new(query) as _
            };

        let forward_result = self.forwarder.forward(forward_req, do_query).await;
        Ok(match forward_result {
            Ok(forward_res) => Some(forward_res),
            Err(e) => {
                error!("Failed to forward sql req but the error is ignored, err:{e}");
                None
            }
        })
    }
}
