// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Implements the TableEngine trait

use std::sync::Arc;

use async_trait::async_trait;
use generic_error::BoxError;
use log::{error, info};
use snafu::{OptionExt, ResultExt};
use table_engine::{
    engine::{
        Close, CloseShardRequest, CloseTableRequest, CreateTableRequest, DropTableRequest,
        OpenShard, OpenShardRequest, OpenShardResult, OpenTableNoCause, OpenTableRequest,
        OpenTableWithCause, Result, TableDef, TableEngine,
    },
    table::{SchemaId, TableRef},
    ANALYTIC_ENGINE_TYPE,
};

use crate::{space::SpaceId, table::TableImpl};
use crate::instance::TableEngineInstance;

pub struct AnalyticTableEngine {
    tableEngineInstance: Arc<TableEngineInstance>,
}

impl Clone for AnalyticTableEngine {
    fn clone(&self) -> Self {
        Self { tableEngineInstance: self.tableEngineInstance.clone() }
    }
}

impl AnalyticTableEngine {
    pub fn new(tableEngineInstance: Arc<TableEngineInstance>) -> Self {
        Self { tableEngineInstance }
    }

    async fn close_tables_of_shard(
        &self,
        close_requests: Vec<CloseTableRequest>, ) -> Vec<Result<String>> {
        if close_requests.is_empty() {
            return Vec::new();
        }

        let mut close_results = Vec::with_capacity(close_requests.len());
        for request in close_requests {
            let result = self
                .close_table(request.clone())
                .await
                .map_err(|e| {
                    error!("Failed to close table, close_request:{request:?}, err:{e}");
                    e
                })
                .map(|_| request.table_name);

            close_results.push(result);
        }

        close_results
    }
}

impl Drop for AnalyticTableEngine {
    fn drop(&mut self) {
        info!("Table engine dropped");
    }
}

#[async_trait]
impl TableEngine for AnalyticTableEngine {
    fn engine_type(&self) -> &str {
        ANALYTIC_ENGINE_TYPE
    }

    async fn close(&self) -> Result<()> {
        info!("Try to close table engine");

        // Close the instance.
        self.tableEngineInstance.close().await.box_err().context(Close)?;

        info!("Table engine closed");

        Ok(())
    }

    async fn create_table(&self, request: CreateTableRequest) -> Result<TableRef> {
        let space_id = build_space_id(request.schema_id);

        info!(
            "Table engine impl create table, space_id:{}, request:{:?}",
            space_id, request
        );

        let space_table = self.tableEngineInstance.create_table(space_id, request).await?;

        let table_impl: TableRef = Arc::new(TableImpl::new(self.tableEngineInstance.clone(), space_table));

        Ok(table_impl)
    }

    async fn drop_table(&self, request: DropTableRequest) -> Result<bool> {
        let space_id = build_space_id(request.schema_id);

        info!(
            "Table engine impl drop table, space_id:{}, request:{:?}",
            space_id, request
        );

        let dropped = self.tableEngineInstance.drop_table(space_id, request).await?;
        Ok(dropped)
    }

    async fn open_table(&self, openTableRequest: OpenTableRequest) -> Result<Option<TableRef>> {
        let shard_id = openTableRequest.shard_id;
        let space_id = build_space_id(openTableRequest.schema_id);
        let table_id = openTableRequest.table_id;

        info!("Table engine impl open table, space_id:{}, request:{:?}",space_id, openTableRequest);

        let table_def = TableDef {
            catalog_name: openTableRequest.catalog_name,
            schema_name: openTableRequest.schema_name,
            schema_id: openTableRequest.schema_id,
            id: table_id,
            name: openTableRequest.table_name,
        };

        // fenquen 只能以shard精度来打开多个表
        let shard_request = OpenShardRequest {
            shard_id,
            table_defs: vec![table_def],
            engineType: openTableRequest.engineType,
        };

        let mut shard_result = self.tableEngineInstance.open_tables_of_same_shard(shard_request).await?;
        let table_opt = shard_result.remove(&table_id).with_context(|| OpenTableNoCause {
            msg: Some(format!("table not exist, table_id:{table_id}, space_id:{space_id}, shard_id:{shard_id}")),
        })?.box_err().context(OpenTableWithCause { msg: None })?;

        let table_opt = table_opt
            .map(|space_table| Arc::new(TableImpl::new(self.tableEngineInstance.clone(), space_table)) as _);

        Ok(table_opt)
    }

    async fn close_table(&self, request: CloseTableRequest) -> Result<()> {
        let space_id = build_space_id(request.schema_id);

        info!(
            "Table engine impl close table, space_id:{}, request:{:?}",
            space_id, request,
        );

        self.tableEngineInstance.close_table(space_id, request).await?;

        Ok(())
    }

    async fn open_shard(&self, request: OpenShardRequest) -> Result<OpenShardResult> {
        let shard_result = self
            .tableEngineInstance
            .open_tables_of_same_shard(request)
            .await
            .box_err()
            .context(OpenShard)?;

        let mut engine_shard_result = OpenShardResult::with_capacity(shard_result.len());
        for (table_id, table_res) in shard_result {
            match table_res.box_err() {
                Ok(Some(space_table)) => {
                    let table_impl = Arc::new(TableImpl::new(self.tableEngineInstance.clone(), space_table));
                    engine_shard_result.insert(table_id, Ok(Some(table_impl)));
                }
                Ok(None) => {
                    engine_shard_result.insert(table_id, Ok(None));
                }
                Err(e) => {
                    engine_shard_result.insert(table_id, Err(e));
                }
            }
        }

        Ok(engine_shard_result)
    }

    async fn close_shard(&self,
                         request: CloseShardRequest) -> Vec<table_engine::engine::Result<String>> {
        let table_defs = request.table_defs;
        let close_requests = table_defs
            .into_iter()
            .map(|def| CloseTableRequest {
                catalog_name: def.catalog_name,
                schema_name: def.schema_name,
                schema_id: def.schema_id,
                table_name: def.name,
                table_id: def.id,
                engine: request.engineType.clone(),
            })
            .collect();

        self.close_tables_of_shard(close_requests).await
    }
}

// fenquen schemaId 和 spaceId 本质相同
/// Generate the space id from the schema id with assumption schema id is unique globally.
#[inline]
pub fn build_space_id(schema_id: SchemaId) -> SpaceId {
    schema_id.as_u32()
}
