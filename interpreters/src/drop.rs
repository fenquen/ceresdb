// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter for drop statements

use async_trait::async_trait;
use macros::define_result;
use query_frontend::plan::DropTablePlan;
use snafu::{ResultExt, Snafu};
use table_engine::engine::TableEngineRef;

use crate::{
    context::InterpreterContext,
    interpreter::{Drop, Interpreter, InterpreterPtr, Output, Result as InterpreterResult},
    table_manipulator::{self, TableManipulatorRef},
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to drop table by table manipulator, err:{}", source))]
    ManipulateTable { source: table_manipulator::Error },
}

define_result!(Error);

/// Drop interpreter
pub struct DropInterpreter {
    ctx: InterpreterContext,
    plan: DropTablePlan,
    table_engine: TableEngineRef,
    table_manipulator: TableManipulatorRef,
}

impl DropInterpreter {
    pub fn create(
        ctx: InterpreterContext,
        plan: DropTablePlan,
        table_engine: TableEngineRef,
        table_manipulator: TableManipulatorRef,
    ) -> InterpreterPtr {
        Box::new(Self {
            ctx,
            plan,
            table_engine,
            table_manipulator,
        })
    }
}

impl DropInterpreter {
    async fn execute_drop(self: Box<Self>) -> Result<Output> {
        self.table_manipulator
            .drop_table(self.ctx, self.plan, self.table_engine)
            .await
            .context(ManipulateTable)
    }
}

// TODO(yingwen): Wrap a method that returns self::Result, simplify some code to
// converting self::Error to super::Error
#[async_trait]
impl Interpreter for DropInterpreter {
    async fn execute(self: Box<Self>) -> InterpreterResult<Output> {
        self.execute_drop().await.context(Drop)
    }
}
