// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Physical execution plan

use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

use async_trait::async_trait;
use datafusion::{
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, display::DisplayableExecutionPlan,
        ExecutionPlan,
    },
    prelude::SessionContext,
};
use macros::define_result;
use snafu::{Backtrace, ResultExt, Snafu};
use table_engine::stream::{FromDfStream, SendableRecordBatchStream};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DataFusion Failed to execute plan, err:{}.\nBacktrace:\n{}", source, backtrace))]
    DataFusionExec {
        partition_count: usize,
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert datafusion stream, err:{}", source))]
    ConvertStream { source: table_engine::stream::Error },
}

define_result!(Error);

pub trait PhysicalPlan: Debug {
    fn execute(&self) -> Result<SendableRecordBatchStream>;

    /// convert internal metrics to string.
    fn metrics_to_string(&self) -> String;
}

pub type PhysicalPlanPtr = Box<dyn PhysicalPlan + Send + Sync>;

pub struct PhysicalPlanImpl {
    dataFusionSessionContext: SessionContext,
    dataFusionExecutionPlan: Arc<dyn ExecutionPlan>,
}

impl PhysicalPlanImpl {
    pub fn with_plan(dataFusionSessionContext: SessionContext, dataFusionExecutionPlan: Arc<dyn ExecutionPlan>) -> Self {
        Self { dataFusionSessionContext, dataFusionExecutionPlan }
    }
}

impl Debug for PhysicalPlanImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionPhysicalPlan").field("plan", &self.dataFusionExecutionPlan).finish()
    }
}

#[async_trait]
impl PhysicalPlan for PhysicalPlanImpl {
    fn execute(&self) -> Result<SendableRecordBatchStream> {
        let task_context = Arc::new(TaskContext::from(&self.dataFusionSessionContext));
        let partition_count = self.dataFusionExecutionPlan.output_partitioning().partition_count();

        let df_stream = if partition_count <= 1 {
            self.dataFusionExecutionPlan.execute(0, task_context).context(DataFusionExec { partition_count })?
        } else {
            // merge into a single partition
            let plan = CoalescePartitionsExec::new(self.dataFusionExecutionPlan.clone());
            // MergeExec must produce a single partition
            assert_eq!(1, plan.output_partitioning().partition_count());
            plan.execute(0, task_context).context(DataFusionExec { partition_count })?
        };

        let stream = FromDfStream::new(df_stream).context(ConvertStream)?;

        Ok(Box::pin(stream))
    }

    fn metrics_to_string(&self) -> String {
        // TODO: set to verbose mode for more details now, maybe we can add a flag to control it.
        DisplayableExecutionPlan::with_metrics(&*self.dataFusionExecutionPlan).indent(true).to_string()
    }
}
