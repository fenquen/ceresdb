// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Logical plans such as select/insert/update/delete

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    fmt::{Debug, Formatter},
    sync::Arc,
};

use common_types::{column_schema::ColumnSchema, row::RowGroup, schema::Schema};
use datafusion::logical_expr::{
    expr::Expr as DfLogicalExpr, logical_plan::LogicalPlan as DataFusionLogicalPlan,
};
use macros::define_result;
use snafu::Snafu;
use table_engine::{partition::PartitionInfo, table::TableRef};

use crate::{ast::ShowCreateObject, container::TableContainer};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unsupported alter table operation."))]
    UnsupportedOperation,

    #[snafu(display("Unsupported column data type, err:{}.", source))]
    UnsupportedDataType { source: common_types::datum::Error },

    #[snafu(display("Unsupported column option:{}.", name))]
    UnsupportedColumnOption { name: String },

    #[snafu(display("Alter primary key is not allowed."))]
    AlterPrimaryKey,
}

define_result!(Error);

// TODO(yingwen): Custom Debug format
/// Logical plan to be processed by interpreters
#[derive(Debug)]
pub enum Plan {
    /// A SQL SELECT plan or other plans related to query
    Query(QueryPlan),
    // TODO(yingwen): Other sql command
    Insert(InsertPlan),
    /// Create table plan
    Create(CreateTablePlan),
    /// Drop table plan
    Drop(DropTablePlan),
    /// Describe table plan
    Describe(DescribeTablePlan),
    /// Alter table plan
    AlterTable(AlterTablePlan),
    /// Show plan
    Show(ShowPlan),
    /// Exists table
    Exists(ExistsTablePlan),
}

pub struct QueryPlan {
    pub dataFusionLogicalPlan: DataFusionLogicalPlan,
    // Contains the TableProviders so we can register the them to ExecutionContext later.
    // Use TableProviderAdapter here so we can get the underlying TableRef and also be
    // able to cast to Arc<dyn TableProvider + Send + Sync>
    pub tables: Arc<TableContainer>,
}

impl Debug for QueryPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryPlan")
            .field("df_plan", &self.dataFusionLogicalPlan)
            .finish()
    }
}

pub struct CreateTablePlan {
    /// Engine
    pub engine: String,
    pub if_not_exists: bool,
    pub tableName: String,
    pub table_schema: Schema,
    pub options: HashMap<String, String>,
    pub partition_info: Option<PartitionInfo>,
}

impl Debug for CreateTablePlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateTablePlan")
            .field("engine", &self.engine)
            .field("if_not_exists", &self.if_not_exists)
            .field("table", &self.tableName)
            .field("table_schema", &self.table_schema)
            .field(
                "options",
                &self
                    .options
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<BTreeMap<String, String>>(),
            )
            .finish()
    }
}

#[derive(Debug)]
pub struct DropTablePlan {
    /// Engine
    pub engine: String,
    /// If exists
    pub if_exists: bool,
    /// Table name
    pub table: String,
    /// Table partition info
    pub partition_info: Option<PartitionInfo>,
}

/// Insert logical plan
#[derive(Debug)]
pub struct InsertPlan {
    /// The table to insert
    pub table: TableRef,
    /// RowGroup to insert
    pub rowGroup: RowGroup,
    /// Column indexes in schema to its default-value-expr which is used to fill values
    pub columnIndex_defaultVal: BTreeMap<usize, DfLogicalExpr>,
}

#[derive(Debug)]
pub struct DescribeTablePlan {
    /// The table to describe
    pub table: TableRef,
}

#[derive(Debug)]
pub enum AlterTableOperation {
    /// Add a new column, the column id will be ignored.
    AddColumn(Vec<ColumnSchema>),
    ModifySetting(HashMap<String, String>),
}

#[derive(Debug)]
pub struct AlterTablePlan {
    /// The table to alter.
    pub table: TableRef,
    // TODO(yingwen): Maybe use smallvec.
    pub operations: AlterTableOperation,
}

#[derive(Debug)]
pub struct ShowCreatePlan {
    /// The table to show.
    pub table: TableRef,
    /// The type to show
    pub obj_type: ShowCreateObject,
}

#[derive(Debug, PartialEq, Eq)]
pub enum QueryType {
    Sql,
    InfluxQL,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ShowTablesPlan {
    /// Like pattern
    pub pattern: Option<String>,
    pub query_type: QueryType,
}

#[derive(Debug)]
pub enum ShowPlan {
    /// show create table
    ShowCreatePlan(ShowCreatePlan),
    /// show tables
    ShowTablesPlan(ShowTablesPlan),
    /// show database
    ShowDatabase,
}

#[derive(Debug)]
pub struct ExistsTablePlan {
    pub exists: bool,
}
