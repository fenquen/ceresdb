// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Logical optimizer
pub mod type_conversion;

use datafusion::{error::DataFusionError, prelude::SessionContext};
use macros::define_result;
use query_frontend::plan::QueryPlan;
use snafu::{Backtrace, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "DataFusion Failed to optimize logical plan, err:{}.\nBacktrace:\n{}",
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

/// LogicalOptimizer transform the QueryPlan into a potentially more efficient
/// plan
pub trait LogicalOptimizer {
    // TODO(yingwen): Maybe support other plans
    fn optimize(&mut self, plan: QueryPlan) -> Result<QueryPlan>;
}

pub struct LogicalOptimizerImpl {
    ctx: SessionContext,
}

impl LogicalOptimizerImpl {
    pub fn with_context(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl LogicalOptimizer for LogicalOptimizerImpl {
    fn optimize(&mut self, plan: QueryPlan) -> Result<QueryPlan> {
        // TODO(yingwen): Avoid clone the plan multiple times during optimization
        let QueryPlan {
            dataFusionLogicalPlan: mut df_plan,
            tables,
        } = plan;
        df_plan = self
            .ctx
            .state()
            .optimize(&df_plan)
            .context(DataFusionOptimize)?;

        Ok(QueryPlan { dataFusionLogicalPlan: df_plan, tables })
    }
}
