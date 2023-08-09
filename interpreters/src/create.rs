// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter for create statements

use std::sync::Arc;
use async_trait::async_trait;
use macros::define_result;
use query_frontend::plan::CreateTablePlan;
use snafu::{ResultExt, Snafu};
use table_engine::engine::TableEngine;

use crate::{
    context::InterpreterContext,
    interpreter::{Create, Interpreter, InterpreterPtr, Output, Result as InterpreterResult},
    table_manipulator::{self},
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
    tableEngine: Arc<dyn TableEngine>,
    tableManipulator: Arc<dyn TableManipulator + Send + Sync>,
}

impl CreateInterpreter {
    pub fn create(interpreterContext: InterpreterContext,
                  createTablePlan: CreateTablePlan,
                  table_engine: Arc<dyn TableEngine>,
                  table_manipulator: Arc<dyn TableManipulator + Send + Sync>) -> InterpreterPtr {
        Box::new(Self {
            interpreterContext,
            createTablePlan,
            tableEngine: table_engine,
            tableManipulator: table_manipulator,
        })
    }
}

impl CreateInterpreter {
    async fn executeCreate(self: Box<Self>) -> Result<Output> {
        self.tableManipulator.createTable(self.interpreterContext,
                                          self.createTablePlan,
                                          self.tableEngine).await.context(ManipulateTable)
    }
}

// TODO(yingwen): Wrap a method that returns self::Result, simplify some code to
// converting self::Error to super::Error
#[async_trait]
impl Interpreter for CreateInterpreter {
    async fn execute(self: Box<Self>) -> InterpreterResult<Output> {
        self.executeCreate().await.context(Create)
    }
}
