// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table engine proxy

use std::sync::Arc;
use async_trait::async_trait;

use crate::{
    engine::{
        CloseShardRequest, CloseTableRequest, CreateTableRequest, DropTableRequest,
        OpenShardRequest, OpenShardResult, OpenTableRequest, TableEngine,
        UnknownEngineType,
    },
    memory::MemoryTableEngine,
    table::TableRef,
    ANALYTIC_ENGINE_TYPE, MEMORY_ENGINE_TYPE,
};

/// Route [CreateTableRequest] to the correct engine by its engine type
pub struct TableEngineProxy {
    /// Memory table engine
    pub memoryTableEngine: MemoryTableEngine,

    /// Analytic table engine
    pub analyticTableEngine: Arc<dyn TableEngine>,
}

#[async_trait]
impl TableEngine for TableEngineProxy {
    fn engine_type(&self) -> &str {
        "TableEngineProxy"
    }

    async fn close(&self) -> crate::engine::Result<()> {
        self.memoryTableEngine.close().await?;
        self.analyticTableEngine.close().await?;

        Ok(())
    }

    async fn createTable(&self, request: CreateTableRequest) -> crate::engine::Result<TableRef> {
        // TODO(yingwen): Use a map
        match request.engine.as_str() {
            MEMORY_ENGINE_TYPE => self.memoryTableEngine.createTable(request).await,
            ANALYTIC_ENGINE_TYPE => self.analyticTableEngine.createTable(request).await,
            engine_type => UnknownEngineType { engine_type }.fail(),
        }
    }

    async fn drop_table(&self, request: DropTableRequest) -> crate::engine::Result<bool> {
        match request.engine.as_str() {
            MEMORY_ENGINE_TYPE => self.memoryTableEngine.drop_table(request).await,
            ANALYTIC_ENGINE_TYPE => self.analyticTableEngine.drop_table(request).await,
            engine_type => UnknownEngineType { engine_type }.fail(),
        }
    }

    /// Open table, return error if table not exists
    async fn open_table(
        &self,
        request: OpenTableRequest,
    ) -> crate::engine::Result<Option<TableRef>> {
        match request.engineType.as_str() {
            MEMORY_ENGINE_TYPE => self.memoryTableEngine.open_table(request).await,
            ANALYTIC_ENGINE_TYPE => self.analyticTableEngine.open_table(request).await,
            engine_type => UnknownEngineType { engine_type }.fail(),
        }
    }

    /// Close table, it is ok to close a closed table.
    async fn close_table(&self, request: CloseTableRequest) -> crate::engine::Result<()> {
        match request.engine.as_str() {
            MEMORY_ENGINE_TYPE => self.memoryTableEngine.close_table(request).await,
            ANALYTIC_ENGINE_TYPE => self.analyticTableEngine.close_table(request).await,
            engine_type => UnknownEngineType { engine_type }.fail(),
        }
    }

    async fn open_shard(
        &self,
        request: OpenShardRequest,
    ) -> crate::engine::Result<OpenShardResult> {
        match request.engineType.as_str() {
            MEMORY_ENGINE_TYPE => self.memoryTableEngine.open_shard(request).await,
            ANALYTIC_ENGINE_TYPE => self.analyticTableEngine.open_shard(request).await,
            engine_type => UnknownEngineType { engine_type }.fail(),
        }
    }

    /// Close tables on same shard.
    async fn close_shard(&self, request: CloseShardRequest) -> Vec<crate::engine::Result<String>> {
        match request.engineType.as_str() {
            MEMORY_ENGINE_TYPE => self.memoryTableEngine.close_shard(request).await,
            ANALYTIC_ENGINE_TYPE => self.analyticTableEngine.close_shard(request).await,
            engine_type => vec![UnknownEngineType { engine_type }.fail()],
        }
    }
}
