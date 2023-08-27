// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter for select statement

use async_trait::async_trait;
use log::debug;
use macros::define_result;
use query_engine::executor::QueryExecutor;
use query_frontend::plan::QueryPlan;
use snafu::{ResultExt, Snafu};

use crate::{
    context::InterpreterContext,
    interpreter::{Interpreter, InterpreterPtr, Output, Result as InterpreterResult, Select},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create query context, err:{}", source))]
    CreateQueryContext { source: crate::context::Error },

    #[snafu(display("Failed to execute logical plan, err:{}", source))]
    ExecutePlan {
        source: query_engine::executor::Error,
    },
}

define_result!(Error);

/// Select interpreter
pub struct SelectInterpreter<T> {
    ctx: InterpreterContext,
    queryPlan: QueryPlan,
    queryExecutor: T,
}

impl<T: QueryExecutor + 'static> SelectInterpreter<T> {
    pub fn create(ctx: InterpreterContext, queryPlan: QueryPlan, queryExecutor: T) -> InterpreterPtr {
        Box::new(Self {
            ctx,
            queryPlan,
            queryExecutor,
        })
    }
}

#[async_trait]
impl<T: QueryExecutor> Interpreter for SelectInterpreter<T> {
    async fn execute(self: Box<Self>) -> InterpreterResult<Output> {
        let request_id = self.ctx.request_id();

        debug!("interpreter execute select begin, request_id:{}, plan:{:?}",request_id, self.queryPlan);

        let query_ctx = self.ctx.new_query_context().context(CreateQueryContext).context(Select)?;

        //let query = Query::new(self.queryPlan);
        let record_batches = self.queryExecutor.executeLogicalPlan(query_ctx, self.queryPlan).await.context(ExecutePlan).context(Select)?;

        debug!("interpreter execute select finish, request_id:{}",request_id);

        Ok(Output::Records(record_batches))
    }
}
