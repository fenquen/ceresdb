// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Query executor

use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
use datafusion::error::DataFusionError;
use common_types::record_batch::RecordBatch;
use futures::TryStreamExt;
use log::{debug, info};
use macros::define_result;
use query_frontend::{plan::QueryPlan, provider::CatalogProviderImpl};
use snafu::{Backtrace, ResultExt, Snafu};
use time_ext::InstantExt;

use crate::{
    config::Config,
    physical_optimizer::PhysicalOptimizer,
    physical_plan::PhysicalPlanPtr,
};
use crate::context::QueryContext;
use crate::physical_plan::PhysicalPlanImpl;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to do logical optimization, err:{}", source))]
    LogicalOptimize {
        //source: crate::logical_optimizer::Error,
        source: DataFusionError,
    },

    #[snafu(display("Failed to do physical optimization, err:{}", source))]
    PhysicalOptimize {
       // source: crate::physical_optimizer::Error,
       source: DataFusionError,
    },

    #[snafu(display("Failed to execute physical plan, err:{}", source))]
    ExecutePhysical { source: crate::physical_plan::Error },

    #[snafu(display("Failed to collect record batch stream, err:{}", source, ))]
    Collect { source: table_engine::stream::Error },

    #[snafu(display("Timeout when execute, err:{}.\nBacktrace:\n{}", source, backtrace))]
    Timeout {
        source: tokio::time::error::Elapsed,
        backtrace: Backtrace,
    },
}

define_result!(Error);

// Use a type alias so that we are able to replace the implementation
pub type RecordBatchVec = Vec<RecordBatch>;

#[derive(Debug)]
pub struct Query {
    /// The query plan
    queryPlan: QueryPlan,
}

impl Query {
    pub fn new(queryPlan: QueryPlan) -> Self {
        Self { queryPlan }
    }
}

/// execute the logical plan
#[async_trait]
pub trait QueryExecutor: Clone + Send + Sync {
    // TODO(yingwen): Maybe return a stream
    /// Execute the query, returning the query results as RecordBatchVec
    ///
    /// REQUIRE: The meta data of tables in query should be found from ContextRef
    async fn executeLogicalPlan(&self, ctx: Arc<QueryContext>, queryPlan: QueryPlan) -> Result<Vec<RecordBatch>>;
}

#[derive(Clone, Default)]
pub struct QueryExecutorImpl {
    config: Config,
}

impl QueryExecutorImpl {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

#[async_trait]
impl QueryExecutor for QueryExecutorImpl {
    async fn executeLogicalPlan(&self, queryContext: Arc<QueryContext>, queryPlan: QueryPlan) -> Result<Vec<RecordBatch>> {
        // register catalogs to datafusion execution context.
        let catalogName_catalogProvider = CatalogProviderImpl::new_adapters(queryPlan.tableContainer.clone());
        let dfSessionContext = queryContext.buildDfSessionContext(&self.config, queryContext.request_id, queryContext.deadline);

        for (catalogName, catalogProvider) in catalogName_catalogProvider {
            dfSessionContext.register_catalog(&catalogName, Arc::new(catalogProvider));
        }

        let beginTime = Instant::now();

        // dataFusionLogicalPlan优化 dataFusion包办
        let dataFusionLogicalPlan = dfSessionContext.state().optimize(&queryPlan.dataFusionLogicalPlan).context(LogicalOptimize)?;
        debug!("executor logical optimization finished, request_id:{}, plan: {:#?}",queryContext.request_id, dataFusionLogicalPlan);

        // dataFusionLogicalPlan变为物理 dataFusion包办
        // create_physical_plan内部的本身也谁对logicalPlan优化 这样就和上边的
        let dataFusionExecutionPlan = dfSessionContext.state().create_physical_plan(&dataFusionLogicalPlan).await.context(PhysicalOptimize)?;
        let physicalPlan = PhysicalPlanImpl::new(dfSessionContext, dataFusionExecutionPlan);
        let physicalPlan:PhysicalPlanPtr = Box::new(physicalPlan);
        debug!("executor physical optimization finished, request_id:{}, physical_plan: {:?}",queryContext.request_id, physicalPlan);

        let stream = physicalPlan.execute().context(ExecutePhysical)?;

        // collect all records in the pool, as the stream may perform some costly calculation
        let recordBatchVec = stream.try_collect().await.context(Collect)?;

        info!("executor executed plan, request_id:{}, cost:{}ms, plan_and_metrics: {}", queryContext.request_id,beginTime.saturating_elapsed().as_millis(),physicalPlan.metrics_to_string());

        Ok(recordBatchVec)
    }
}