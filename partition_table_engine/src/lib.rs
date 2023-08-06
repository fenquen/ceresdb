// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partition table engine implementations

mod error;
mod metrics;
mod partition;

use std::sync::Arc;

use async_trait::async_trait;
use generic_error::BoxError;
use snafu::{OptionExt, ResultExt};
use table_engine::{
    engine::{
        CloseShardRequest, CloseTableRequest, CreateTableRequest, DropTableRequest,
        OpenShardRequest, OpenShardResult, OpenTableRequest, Result, TableEngine, Unexpected,
        UnexpectedNoCause,
    },
    remote::RemoteEngineRef,
    table::TableRef,
    PARTITION_TABLE_ENGINE_TYPE,
};
use table_engine::remote::RemoteEngine;

use crate::partition::{PartitionTableImpl, TableData};

/// Partition table engine implementation.
pub struct PartitionTableEngine {
    remote_engine_ref: Arc<dyn RemoteEngine>,
}

impl PartitionTableEngine {
    pub fn new(remote_engine_ref: Arc<dyn RemoteEngine>) -> Self {
        Self { remote_engine_ref }
    }
}

#[async_trait]
impl TableEngine for PartitionTableEngine {
    fn engine_type(&self) -> &str {
        PARTITION_TABLE_ENGINE_TYPE
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    async fn create_table(&self, request: CreateTableRequest) -> Result<TableRef> {
        let table_data = TableData {
            catalog_name: request.catalog_name,
            schema_name: request.schema_name,
            table_name: request.table_name,
            table_id: request.table_id,
            table_schema: request.table_schema,
            partition_info: request.partition_info.context(UnexpectedNoCause {
                msg: "partition info not found",
            })?,
            options: request.options,
            engine_type: request.engine,
        };
        Ok(Arc::new(
            PartitionTableImpl::new(table_data, self.remote_engine_ref.clone())
                .box_err()
                .context(Unexpected)?,
        ))
    }

    async fn drop_table(&self, _request: DropTableRequest) -> Result<bool> {
        Ok(true)
    }

    async fn open_table(&self, _request: OpenTableRequest) -> Result<Option<TableRef>> {
        Ok(None)
    }

    async fn close_table(&self, _request: CloseTableRequest) -> Result<()> {
        Ok(())
    }

    async fn open_shard(&self, _request: OpenShardRequest) -> Result<OpenShardResult> {
        Ok(OpenShardResult::default())
    }

    async fn close_shard(&self, _request: CloseShardRequest) -> Vec<Result<String>> {
        vec![Ok("".to_string())]
    }
}
