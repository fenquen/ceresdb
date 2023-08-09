// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use catalog::{
    schema::{CreateOptions, CreateTableRequest, DropOptions, DropTableRequest},
    table_operator::TableOperator,
};
use common_types::table::DEFAULT_SHARD_ID;
use query_frontend::plan::{CreateTablePlan, DropTablePlan};
use snafu::{ensure, ResultExt};
use table_engine::engine::{TableEngineRef, TableState};

use crate::{
    context::InterpreterContext,
    interpreter::Output,
    table_manipulator::{
        PartitionTableNotSupported, Result, TableManipulator, TableOperator as TableOperatorErr,
    },
};

pub struct TableManipulatorCatalogBased {
    table_operator: TableOperator,
}

impl TableManipulatorCatalogBased {
    pub fn new(table_operator: TableOperator) -> Self {
        Self { table_operator }
    }
}

#[async_trait]
impl TableManipulator for TableManipulatorCatalogBased {
    async fn createTable(&self,
                         ctx: InterpreterContext,
                         plan: CreateTablePlan,
                         table_engine: TableEngineRef) -> Result<Output> {
        ensure!(plan.partition_info.is_none(),PartitionTableNotSupported { table: plan.tableName });

        let default_catalog = ctx.default_catalog();
        let default_schema = ctx.default_schema();

        let CreateTablePlan {
            engine,
            tableName: table,
            table_schema,
            if_not_exists,
            options,
            ..
        } = plan;

        let request = CreateTableRequest {
            catalogName: default_catalog.to_string(),
            schemaName: default_schema.to_string(),
            table_name: table.clone(),
            table_id: None,
            table_schema,
            engine,
            options,
            state: TableState::Stable,
            shard_id: DEFAULT_SHARD_ID,
            partition_info: None,
        };

        let opts = CreateOptions {
            tableEngine: table_engine,
            create_if_not_exists: if_not_exists,
        };

        let _ = self.table_operator.create_table_on_shard(request, opts).await.context(TableOperatorErr)?;

        Ok(Output::AffectedRows(0))
    }

    async fn drop_table(&self,
                        ctx: InterpreterContext,
                        plan: DropTablePlan,
                        table_engine: TableEngineRef, ) -> Result<Output> {
        let default_catalog = ctx.default_catalog();
        let default_schema = ctx.default_schema();

        let table = plan.table;
        let request = DropTableRequest {
            catalog_name: default_catalog.to_string(),
            schema_name: default_schema.to_string(),
            table_name: table.clone(),
            engine: plan.engine,
        };

        let opts = DropOptions { table_engine };

        self.table_operator.drop_table_on_shard(request, opts).await.context(TableOperatorErr)?;

        Ok(Output::AffectedRows(0))
    }
}
