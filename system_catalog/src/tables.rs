// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

/// implementation of system table: Tables
/// For example `SELECT * FROM system.public.tables`
use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use catalog::{manager::ManagerRef, schema::SchemaRef, CatalogRef};
use common_types::{
    column_schema,
    datum::{Datum, DatumKind},
    record_batch::RecordBatchWithKeyBuilder,
    row::Row,
    schema,
    schema::Schema,
    time::Timestamp,
};
use generic_error::BoxError;
use snafu::ResultExt;
use table_engine::{
    stream::SendableRecordBatchStream,
    table::{ReadRequest, TableId, TableRef},
};

use crate::{OneRecordBatchStream, SystemTable, TABLES_TABLE_ID, TABLES_TABLE_NAME};

/// Timestamp of entry
pub const ENTRY_TIMESTAMP: Timestamp = Timestamp::new(0);

/// Build a new table schema for tables
fn tables_schema() -> Schema {
    schema::Builder::with_capacity(6)
        .auto_increment_column_id(true)
        .add_key_column(
            column_schema::Builder::new("timestamp".to_string(), DatumKind::Timestamp)
                .is_nullable(false)
                .is_tag(false)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_key_column(
            column_schema::Builder::new("catalog".to_string(), DatumKind::String)
                .is_nullable(false)
                .is_tag(false)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_key_column(
            column_schema::Builder::new("schema".to_string(), DatumKind::String)
                .is_nullable(false)
                .is_tag(false)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_key_column(
            column_schema::Builder::new("table_name".to_string(), DatumKind::String)
                .is_nullable(false)
                .is_tag(false)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("table_id".to_string(), DatumKind::UInt64)
                .is_nullable(false)
                .is_tag(false)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("engine".to_string(), DatumKind::String)
                .is_nullable(false)
                .is_tag(false)
                .build()
                .unwrap(),
        )
        .unwrap()
        .build()
        .unwrap()
}

pub struct Tables {
    schema: Schema,
    catalog_manager: ManagerRef,
}

impl Debug for Tables {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SysTables")
            .field("schema", &self.schema)
            .finish()
    }
}

impl Tables {
    pub fn new(catalog_manager: ManagerRef) -> Self {
        Self {
            schema: tables_schema(),
            catalog_manager,
        }
    }

    #[allow(clippy::wrong_self_convention)]
    fn from_table(&self, catalog: CatalogRef, schema: SchemaRef, table: TableRef) -> Row {
        let mut datums = Vec::with_capacity(self.schema.num_columns());
        datums.push(Datum::Timestamp(ENTRY_TIMESTAMP));
        datums.push(Datum::from(catalog.name()));
        datums.push(Datum::from(schema.name()));
        datums.push(Datum::from(table.name()));
        datums.push(Datum::from(table.id().as_u64()));
        datums.push(Datum::from(table.engine_type()));
        Row::from_datums(datums)
    }
}

#[async_trait]
impl SystemTable for Tables {
    fn name(&self) -> &str {
        TABLES_TABLE_NAME
    }

    fn id(&self) -> TableId {
        TABLES_TABLE_ID
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    async fn read(
        &self,
        request: ReadRequest,
    ) -> table_engine::table::Result<SendableRecordBatchStream> {
        let catalogs = self
            .catalog_manager
            .all_catalogs()
            .box_err()
            .context(table_engine::table::Scan { table: self.name() })?;
        let projected_record_schema = request.projectedSchema.to_record_schema_with_key();
        let mut builder = RecordBatchWithKeyBuilder::new(projected_record_schema);

        let projector = request
            .projectedSchema
            .tryProjectWithKey(&self.schema)
            .expect("Should succeed to try_project_key of sys_tables");
        for catalog in &catalogs {
            for schema in &catalog
                .all_schemas()
                .box_err()
                .context(table_engine::table::Scan { table: self.name() })?
            {
                for table in &schema
                    .all_tables()
                    .box_err()
                    .context(table_engine::table::Scan { table: self.name() })?
                {
                    let row = self.from_table(catalog.clone(), schema.clone(), table.clone());
                    let projected_row = projector.project_row(&row, Vec::new());
                    builder
                        .append_row(projected_row)
                        .box_err()
                        .context(table_engine::table::Scan { table: self.name() })?;
                }
            }
        }
        let record_batch = builder.build().unwrap().intoRecordBatch();
        Ok(Box::pin(OneRecordBatchStream {
            schema: self.schema.clone().to_record_schema(),
            record_batch: Some(record_batch),
        }))
    }
}
