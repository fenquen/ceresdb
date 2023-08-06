// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use common_types::schema::SchemaEncoder;
use generic_error::BoxError;
use log::info;
use meta_client::{
    types::{CreateTableRequest, DropTableRequest, PartitionTableInfo},
    MetaClientRef,
};
use query_frontend::plan::{CreateTablePlan, DropTablePlan};
use snafu::ResultExt;
use table_engine::{
    engine::TableEngineRef,
    partition::{format_sub_partition_table_name, PartitionInfo},
};

use crate::{
    context::InterpreterContext,
    interpreter::Output,
    table_manipulator::{CreateWithCause, DropWithCause, Result, TableManipulator},
};

pub struct TableManipulatorMetaBased {
    meta_client: MetaClientRef,
}

impl TableManipulatorMetaBased {
    pub fn new(meta_client: MetaClientRef) -> Self {
        Self { meta_client }
    }
}

#[async_trait]
impl TableManipulator for TableManipulatorMetaBased {
    async fn create_table(
        &self,
        ctx: InterpreterContext,
        plan: CreateTablePlan,
        _table_engine: TableEngineRef,
    ) -> Result<Output> {
        let encoded_schema = SchemaEncoder::default()
            .encode(&plan.table_schema)
            .box_err()
            .with_context(|| CreateWithCause {
                msg: format!("fail to encode table schema, ctx:{ctx:?}, plan:{plan:?}"),
            })?;

        let partition_table_info = create_partition_table_info(&plan.table, &plan.partition_info);

        let req = CreateTableRequest {
            schema_name: ctx.default_schema().to_string(),
            name: plan.table,
            encoded_schema,
            engine: plan.engine,
            create_if_not_exist: plan.if_not_exists,
            options: plan.options,
            partition_table_info,
        };

        let resp = self
            .meta_client
            .create_table(req.clone())
            .await
            .box_err()
            .with_context(|| CreateWithCause {
                msg: format!("failed to create table by meta client, req:{req:?}"),
            })?;

        info!(
            "Create table by meta successfully, req:{:?}, resp:{:?}",
            req, resp
        );

        Ok(Output::AffectedRows(0))
    }

    async fn drop_table(
        &self,
        ctx: InterpreterContext,
        plan: DropTablePlan,
        _table_engine: TableEngineRef,
    ) -> Result<Output> {
        let partition_table_info = create_partition_table_info(&plan.table, &plan.partition_info);

        let req = DropTableRequest {
            schema_name: ctx.default_schema().to_string(),
            name: plan.table,
            partition_table_info,
        };

        let resp = self
            .meta_client
            .drop_table(req.clone())
            .await
            .box_err()
            .context(DropWithCause {
                msg: format!("failed to create table by meta client, req:{req:?}"),
            })?;

        info!(
            "Drop table by meta successfully, req:{:?}, resp:{:?}",
            req, resp
        );

        Ok(Output::AffectedRows(0))
    }
}

fn create_partition_table_info(
    table_name: &str,
    partition_info: &Option<PartitionInfo>,
) -> Option<PartitionTableInfo> {
    if let Some(info) = partition_info {
        let sub_table_names = info
            .get_definitions()
            .iter()
            .map(|def| format_sub_partition_table_name(table_name, &def.name))
            .collect::<Vec<String>>();
        Some(PartitionTableInfo {
            sub_table_names,
            partition_info: info.clone(),
        })
    } else {
        None
    }
}
