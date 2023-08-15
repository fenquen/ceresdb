// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! The query planner adapter provides some planner extensions of datafusion.

use std::sync::Arc;

use datafusion::{
    execution::context::{QueryPlanner, SessionState},
    logical_expr::logical_plan::LogicalPlan,
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
};

pub mod prom_align;

use async_trait::async_trait;

pub struct QueryPlannerImpl;

#[async_trait]
impl QueryPlanner for QueryPlannerImpl {
    async fn create_physical_plan(&self,
                                  logicalPlan: &LogicalPlan,
                                  sessionState: &SessionState) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> = vec![
            Arc::new(prom_align::PromAlignPlanner),
            Arc::new(influxql_query::exec::context::IOxExtensionPlanner {}),
        ];

        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(extension_planners);
        physical_planner.create_physical_plan(logicalPlan, sessionState).await
    }
}
