// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Physical query optimizer

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    error::DataFusionError, physical_optimizer::optimizer::PhysicalOptimizerRule,
    prelude::SessionContext,
};
use datafusion::logical_expr::LogicalPlan;
use macros::define_result;
use snafu::{Backtrace, ResultExt, Snafu};

use crate::{
    physical_optimizer::{
        coalesce_batches::CoalesceBatchesAdapter, repartition::RepartitionAdapter,
    },
    physical_plan::{PhysicalPlanImpl, PhysicalPlanPtr},
};

pub mod coalesce_batches;
pub mod repartition;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "DataFusion Failed to optimize physical plan, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    // TODO(yingwen): Should we carry plan in this context?
    DataFusionOptimize {
        source: DataFusionError,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// physical query optimizer that converts a logical plan to a physical plan
#[async_trait]
pub trait PhysicalOptimizer {
    async fn optimize(&mut self, logical_plan: LogicalPlan) -> Result<PhysicalPlanPtr>;
}

pub struct PhysicalOptimizerImpl {
    ctx: SessionContext,
}

impl PhysicalOptimizerImpl {
    pub fn with_context(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl PhysicalOptimizer for PhysicalOptimizerImpl {
    async fn optimize(&mut self, dataFusionLogicalPlan: LogicalPlan) -> Result<PhysicalPlanPtr> {
        let exec_plan = self.ctx.state().create_physical_plan(&dataFusionLogicalPlan).await.context(DataFusionOptimize)?;
        let physical_plan = PhysicalPlanImpl::new(self.ctx.clone(), exec_plan);

        Ok(Box::new(physical_plan))
    }
}

pub type OptimizeRuleRef = Arc<dyn PhysicalOptimizerRule + Send + Sync>;

/// The default optimize rules of the datafusion is not all suitable for our
/// cases so the adapters may change the default rules(normally just decide
/// whether to apply the rule according to the specific plan).
pub trait Adapter {
    /// May change the original rule into the custom one.
    fn may_adapt(original_rule: OptimizeRuleRef) -> OptimizeRuleRef;
}

pub fn may_adapt_optimize_rule(
    original_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
) -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
    CoalesceBatchesAdapter::may_adapt(RepartitionAdapter::may_adapt(original_rule))
}
