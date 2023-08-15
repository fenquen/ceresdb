// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Logical optimizer
pub mod type_conversion;

use datafusion::{error::DataFusionError, prelude::SessionContext};
use macros::define_result;
use query_frontend::plan::QueryPlan;
use snafu::{Backtrace, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("dataFusion Failed to optimize logical plan, err:{}.\nBacktrace:\n{}", source, backtrace))]
    // TODO(yingwen): Should we carry plan in this context?
    DataFusionOptimize {
        source: DataFusionError,
        backtrace: Backtrace,
    },
}

define_result!(Error);