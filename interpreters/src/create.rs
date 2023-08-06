// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter for create statements

use std::sync::Arc;
use async_trait::async_trait;
use macros::define_result;
use query_frontend::plan::CreateTablePlan;
use snafu::{ResultExt, Snafu};
use table_engine::engine::{TableEngine, TableEngineRef};

use crate::{
    context::InterpreterContext,
    interpreter::{Create, Interpreter, InterpreterPtr, Output, Result as InterpreterResult},
    table_manipulator::{self, TableManipulatorRef},
};
use crate::table_manipulator::TableManipulator;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub (crate)))]
pub enum Error {
    #[snafu(display("Failed to create table by table manipulator, err:{}", source))]
    ManipulateTable { source: table_manipulator::Error },
}

define_result!(Error);

/// Create interpreter
pub struct CreateInterpreter {
    interpreterContext: InterpreterContext,
    createTablePlan: CreateTablePlan,
    table_engine: Arc<dyn TableEngine>,
    table_manipulator: Arc<dyn TableManipulator + Send + Sync>,
}

impl CreateInterpreter {
    pub fn create(interpreterContext: InterpreterContext,
                  createTablePlan: CreateTablePlan,
                  table_engine: Arc<dyn TableEngine>,
                  table_manipulator: Arc<dyn TableManipulator + Send + Sync>) -> InterpreterPtr {
        Box::new(Self {
            interpreterContext,
            createTablePlan,
            table_engine,
            table_manipulator,
        })
    }
}

impl CreateInterpreter {
    async fn execute_create(self: Box<Self>) -> Result<Output> {
        self.table_manipulator.create_table(self.interpreterContext,
                                            self.createTablePlan,
                                            self.table_engine).await.context(ManipulateTable)
    }
}

// TODO(yingwen): Wrap a method that returns self::Result, simplify some code to
// converting self::Error to super::Error
#[async_trait]
impl Interpreter for CreateInterpreter {
    async fn execute(self: Box<Self>) -> InterpreterResult<Output> {
        self.execute_create().await.context(Create)
    }
}
