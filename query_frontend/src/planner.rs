// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Planner converts a SQL AST into logical plans

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    convert::TryFrom,
    mem,
    ops::ControlFlow,
    sync::Arc,
};

use arrow::{
    compute::can_cast_types,
    datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
    error::ArrowError,
};
use catalog::consts::{DEFAULT_CATALOG, DEFAULT_SCHEMA};
use ceresdbproto::storage::{value::Value as PbValue, WriteTableRequest};
use cluster::config::SchemaConfig;
use common_types::{
    column_schema::{self, ColumnSchema},
    datum::{Datum, DatumKind},
    request_id::RequestId,
    row::{RowGroup, RowGroupBuilder},
    schema::{self, Builder as SchemaBuilder, Schema, TSID_COLUMN},
};
use datafusion::{
    common::{DFField, DFSchema},
    error::DataFusionError,
    optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext},
    physical_expr::{create_physical_expr, execution_props::ExecutionProps},
    sql::{
        planner::{ParserOptions, PlannerContext, SqlToRel},
        ResolvedTableReference,
    },
};
use generic_error::GenericError;
use influxql_parser::statement::Statement as InfluxqlStatement;
use log::{debug, info, trace};
use macros::define_result;
use prom_remote_api::types::Query as PromRemoteQuery;
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use sqlparser::ast::{
    visit_statements_mut, ColumnDef, ColumnOption, Expr, Expr as SqlExpr, Ident, Query, SelectItem,
    SetExpr, SqlOption, Statement as SqlStatement, TableConstraint, UnaryOperator, Value, Values,
};
use table_engine::table::TableRef;

use crate::{
    ast::{
        AlterAddColumn, AlterModifySetting, CreateTable, DescribeTable, DropTable, ExistsTable,
        ShowCreate, ShowTables, Statement, TableName,
    },
    container::TableReference,
    parser,
    partition::PartitionParser,
    plan::{
        AlterTableOperation, AlterTablePlan, CreateTablePlan, DescribeTablePlan, DropTablePlan,
        ExistsTablePlan, InsertPlan, Plan, QueryPlan, QueryType, ShowCreatePlan, ShowPlan,
        ShowTablesPlan,
    },
    promql::{remote_query_to_plan, ColumnNames, Expr as PromExpr, RemoteQueryPlan},
    provider::{MetaAndContextProvider, MetaProvider},
};

// We do not carry backtrace in sql error because it is mainly used in server
// handler and the error is usually caused by invalid/unsupported sql, which
// should be easy to find out the reason.
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("Failed to generate datafusion plan, err:{}", source))]
    DatafusionPlan { source: DataFusionError },

    #[snafu(display("Failed to create datafusion schema, err:{}", source))]
    CreateDatafusionSchema { source: DataFusionError },

    #[snafu(display("Failed to merge arrow schema, err:{}", source))]
    MergeArrowSchema { source: ArrowError },

    #[snafu(display("Failed to generate datafusion expr, err:{}", source))]
    DatafusionExpr { source: DataFusionError },

    #[snafu(display(
    "Failed to get data type from datafusion physical expr, err:{}",
    source
    ))]
    DatafusionDataType { source: DataFusionError },

    // Statement is too large and complicate to carry in Error, so we
    // only return error here, so the caller should attach sql to its
    // error context
    #[snafu(display("Unsupported SQL statement"))]
    UnsupportedStatement,

    #[snafu(display("Create table name is empty"))]
    CreateTableNameEmpty,

    #[snafu(display("Only support non-column-input expr in default-value-option, column name:{} default value:{}", name, default_value))]
    CreateWithComplexDefaultValue { name: String, default_value: Expr },

    #[snafu(display("Table must contain timestamp constraint"))]
    RequireTimestamp,

    #[snafu(display(
    "Table must contain only one timestamp key and it's data type must be TIMESTAMP"
    ))]
    InvalidTimestampKey,

    #[snafu(display("Invalid unsign type: {}.\nBacktrace:\n{}", kind, backtrace))]
    InvalidUnsignType {
        kind: DatumKind,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Undefined column is used in primary key, column name:{}.\nBacktrace:\n{}",
    name,
    backtrace
    ))]
    UndefinedColumnInPrimaryKey { name: String, backtrace: Backtrace },

    #[snafu(display("Primary key not found, column name:{}", name))]
    PrimaryKeyNotFound { name: String },

    #[snafu(display(
    "Duplicate definitions of primary key are found, first:{:?}, second:{:?}.\nBacktrace:\n{:?}",
    first,
    second,
    backtrace,
    ))]
    DuplicatePrimaryKey {
        first: Vec<Ident>,
        second: Vec<Ident>,
        backtrace: Backtrace,
    },

    #[snafu(display("Tag column not found, name:{}", name))]
    TagColumnNotFound { name: String },

    #[snafu(display("timestamp key column can not be tag, name:{}.\nBacktrace:\n{:?}", name, backtrace))]
    TimestampKeyTag { name: String, backtrace: Backtrace },

    #[snafu(display("Timestamp column not found, name:{}", name))]
    TimestampColumnNotFound { name: String },

    #[snafu(display("{} is a reserved column name", name))]
    ColumnNameReserved { name: String },

    #[snafu(display("Invalid create table name, err:{}", source))]
    InvalidCreateTableName { source: DataFusionError },

    #[snafu(display("Failed to build schema, err:{}", source))]
    BuildTableSchema { source: common_types::schema::Error },

    #[snafu(display("Unsupported SQL data type, err:{}", source))]
    UnsupportedDataType { source: common_types::datum::Error },

    #[snafu(display("Invalid column schema, column_name:{}, err:{}", column_name, source))]
    InvalidColumnSchema {
        column_name: String,
        source: column_schema::Error,
    },

    #[snafu(display("Invalid table name, err:{}", source))]
    InvalidTableName { source: DataFusionError },

    #[snafu(display("Table not found, table:{}", name))]
    TableNotFound { name: String },

    #[snafu(display("Column is not null, table:{}, column:{}", table, column))]
    InsertMissingColumn { table: String, column: String },

    #[snafu(display("Column is reserved, table:{}, column:{}", table, column))]
    InsertReservedColumn { table: String, column: String },

    #[snafu(display("Unknown insert column, name:{}", name))]
    UnknownInsertColumn { name: String },

    #[snafu(display("Insert values not enough, len:{}, index:{}", len, index))]
    InsertValuesNotEnough { len: usize, index: usize },

    #[snafu(display("Invalid insert stmt, contains duplicate columns"))]
    InsertDuplicateColumns,

    #[snafu(display("Invalid insert stmt, source should be a set"))]
    InsertSourceBodyNotSet,

    #[snafu(display("invalid insert stmt, source expr is not value, source_expr:{:?}.\nBacktrace:\n{}", source_expr, backtrace, ))]
    InsertExprNotValue {
        source_expr: Expr,
        backtrace: Backtrace,
    },

    #[snafu(display("invalid insert stmt, source expr has no valid negative value, source_expr:{:?}.\nBacktrace:\n{}", source_expr, backtrace))]
    InsertExprNoNegativeValue {
        source_expr: Expr,
        backtrace: Backtrace,
    },

    #[snafu(display("Insert Failed to convert value, err:{}", source))]
    InsertConvertValue { source: common_types::datum::Error },

    #[snafu(display("Failed to build row, err:{}", source))]
    BuildRow { source: common_types::row::Error },

    #[snafu(display("MetaProvider Failed to find table, err:{}", source))]
    MetaProviderFindTable { source: crate::provider::Error },

    #[snafu(display("Failed to find meta during planning, err:{}", source))]
    FindMeta { source: crate::provider::Error },

    #[snafu(display("Invalid alter table operation, err:{}", source))]
    InvalidAlterTableOperation { source: crate::plan::Error },

    #[snafu(display("Unsupported sql option, value:{}", value))]
    UnsupportedOption { value: String },

    #[snafu(display("Failed to build plan from promql, error:{}", source))]
    BuildPromPlanError { source: crate::promql::Error },

    #[snafu(display(
    "Failed to cast default value expr to column type, expr:{}, from:{}, to:{}",
    expr,
    from,
    to
    ))]
    InvalidDefaultValueCoercion {
        expr: Expr,
        from: ArrowDataType,
        to: ArrowDataType,
    },

    #[snafu(display(
    "Failed to parse partition statement to partition info, msg:{}, err:{}",
    msg,
    source,
    ))]
    ParsePartitionWithCause { msg: String, source: GenericError },

    #[snafu(display("Unsupported partition method, msg:{}", msg, ))]
    UnsupportedPartition { msg: String },

    #[snafu(display("Failed to build plan, msg:{}", msg))]
    InvalidWriteEntry { msg: String },

    #[snafu(display("Failed to build influxql plan, err:{}", source))]
    BuildInfluxqlPlan {
        source: crate::influxql::error::Error,
    },
}

define_result!(Error);

const DEFAULT_QUOTE_CHAR: char = '`';
const DEFAULT_PARSER_OPTS: ParserOptions = ParserOptions {
    parse_float_as_decimal: false,
    enable_ident_normalization: false,
};

/// Planner produces logical plans from SQL AST
// TODO(yingwen): Rewrite Planner instead of using datafusion's planner
pub struct Planner<'a, P: MetaProvider> {
    metaProvider: &'a P,
    request_id: RequestId,
    read_parallelism: usize,
}

impl<'a, P: MetaProvider> Planner<'a, P> {
    /// Create a new logical planner
    pub fn new(metaProvider: &'a P, request_id: RequestId, read_parallelism: usize) -> Self {
        Self {
            metaProvider,
            request_id,
            read_parallelism,
        }
    }

    /// Create a logical plan from Statement
    ///
    /// Takes the ownership of statement because some statements like INSERT
    /// statements contains lots of data
    pub fn statementToPlan(&self, statement: Statement) -> Result<Plan> {
        trace!("statement to plan, request_id:{}, statement:{:?}",self.request_id,statement);

        let adapter = MetaAndContextProvider::new(self.metaProvider, self.read_parallelism);
        // SqlToRel needs to hold the reference to adapter, thus we can't both holds the
        // adapter and the SqlToRel in Planner, which is a self-referential
        // case. We wrap a PlannerDelegate to workaround this and avoid the usage of pin.
        let plannerDelegate = PlannerDelegate::new(adapter);

        match statement {
            Statement::Standard(s) => plannerDelegate.sqlStatementToLogicalPlan(*s),
            Statement::Create(s) => plannerDelegate.createTableToPlan(*s),
            Statement::Drop(s) => plannerDelegate.drop_table_to_plan(s),
            Statement::Describe(s) => plannerDelegate.describe_table_to_plan(s),
            Statement::AlterModifySetting(s) => plannerDelegate.alter_modify_setting_to_plan(s),
            Statement::AlterAddColumn(s) => plannerDelegate.alter_add_column_to_plan(s),
            Statement::ShowCreate(s) => plannerDelegate.show_create_to_plan(s),
            Statement::ShowTables(s) => plannerDelegate.show_tables_to_plan(s),
            Statement::ShowDatabases => plannerDelegate.show_databases_to_plan(),
            Statement::Exists(s) => plannerDelegate.exists_table_to_plan(s),
        }
    }

    pub fn promql_expr_to_plan(&self, expr: PromExpr) -> Result<(Plan, Arc<ColumnNames>)> {
        let adapter = MetaAndContextProvider::new(self.metaProvider, self.read_parallelism);
        // SqlToRel needs to hold the reference to adapter, thus we can't both holds the
        // adapter and the SqlToRel in Planner, which is a self-referential
        // case. We wrap a PlannerDelegate to workaround this and avoid the usage of
        // pin.
        let planner = PlannerDelegate::new(adapter);

        expr.to_plan(planner.metaAndContextProvider, self.read_parallelism)
            .context(BuildPromPlanError)
    }

    pub fn remote_prom_req_to_plan(&self, query: PromRemoteQuery) -> Result<RemoteQueryPlan> {
        let adapter = MetaAndContextProvider::new(self.metaProvider, self.read_parallelism);
        let planner = PlannerDelegate::new(adapter);

        remote_query_to_plan(query, planner.metaAndContextProvider).context(BuildPromPlanError)
    }

    pub fn influxql_stmt_to_plan(&self, statement: InfluxqlStatement) -> Result<Plan> {
        let adapter = MetaAndContextProvider::new(self.metaProvider, self.read_parallelism);
        let planner = crate::influxql::planner::Planner::new(adapter);
        planner
            .statement_to_plan(statement)
            .context(BuildInfluxqlPlan)
    }

    pub fn write_req_to_plan(
        &self,
        schema_config: &SchemaConfig,
        write_table: &WriteTableRequest,
    ) -> Result<Plan> {
        Ok(Plan::Create(CreateTablePlan {
            engine: schema_config.default_engine_type.clone(),
            if_not_exists: true,
            tableName: write_table.table.clone(),
            table_schema: build_schema_from_write_table_request(schema_config, write_table)?,
            options: HashMap::default(),
            partition_info: None,
        }))
    }
}

pub fn build_column_schema(
    column_name: &str,
    data_type: DatumKind,
    is_tag: bool,
) -> Result<ColumnSchema> {
    let builder = column_schema::Builder::new(column_name.to_string(), data_type)
        .is_nullable(true)
        .is_tag(is_tag);

    builder.build().with_context(|| InvalidColumnSchema {
        column_name: column_name.to_string(),
    })
}

pub fn build_schema_from_write_table_request(
    schema_config: &SchemaConfig,
    write_table_req: &WriteTableRequest,
) -> Result<Schema> {
    let WriteTableRequest {
        table,
        field_names,
        tag_names,
        entries: write_entries,
    } = write_table_req;

    let mut schema_builder =
        SchemaBuilder::with_capacity(field_names.len()).auto_increment_column_id(true);

    ensure!(
        !write_entries.is_empty(),
        InvalidWriteEntry {
            msg: "empty write entries".to_string()
        }
    );

    let mut name_column_map: BTreeMap<_, ColumnSchema> = BTreeMap::new();
    for write_entry in write_entries {
        // parse tags
        for tag in &write_entry.tags {
            let name_index = tag.name_index as usize;
            ensure!(
                name_index < tag_names.len(),
                InvalidWriteEntry {
                    msg: format!(
                        "tag {tag:?} is not found in tag_names:{tag_names:?}, table:{table}",
                    ),
                }
            );

            let tag_name = &tag_names[name_index];

            let tag_value = tag
                .value
                .as_ref()
                .with_context(|| InvalidWriteEntry {
                    msg: format!("Tag({tag_name}) value is needed, table_name:{table} "),
                })?
                .value
                .as_ref()
                .with_context(|| InvalidWriteEntry {
                    msg: format!("Tag({tag_name}) value type is not supported, table_name:{table}"),
                })?;

            let data_type = try_get_data_type_from_value(tag_value)?;

            if let Some(column_schema) = name_column_map.get(tag_name) {
                ensure_data_type_compatible(table, tag_name, true, data_type, column_schema)?;
            }

            let column_schema = build_column_schema(tag_name, data_type, true)?;
            name_column_map.insert(tag_name, column_schema);
        }

        // parse fields
        for field_group in &write_entry.field_groups {
            for field in &field_group.fields {
                if (field.name_index as usize) < field_names.len() {
                    let field_name = &field_names[field.name_index as usize];
                    let field_value = field
                        .value
                        .as_ref()
                        .with_context(|| InvalidWriteEntry {
                            msg: format!("Field({field_name}) value is needed, table:{table}"),
                        })?
                        .value
                        .as_ref()
                        .with_context(|| InvalidWriteEntry {
                            msg: format!(
                                "Field({field_name}) value type is not supported, table:{table}"
                            ),
                        })?;

                    let data_type = try_get_data_type_from_value(field_value)?;

                    if let Some(column_schema) = name_column_map.get(field_name) {
                        ensure_data_type_compatible(
                            table,
                            field_name,
                            false,
                            data_type,
                            column_schema,
                        )?;
                    }
                    let column_schema = build_column_schema(field_name, data_type, false)?;
                    name_column_map.insert(field_name, column_schema);
                }
            }
        }
    }

    // Timestamp column will be the last column
    let timestamp_column_schema = column_schema::Builder::new(
        schema_config.default_timestamp_column_name.clone(),
        DatumKind::Timestamp,
    )
        .is_nullable(false)
        .build()
        .with_context(|| InvalidColumnSchema {
            column_name: schema_config.default_timestamp_column_name.clone(),
        })?;

    // Use (tsid, timestamp) as primary key.
    let tsid_column_schema =
        column_schema::Builder::new(TSID_COLUMN.to_string(), DatumKind::UInt64)
            .is_nullable(false)
            .build()
            .with_context(|| InvalidColumnSchema {
                column_name: TSID_COLUMN.to_string(),
            })?;

    schema_builder = schema_builder
        .add_key_column(tsid_column_schema)
        .context(BuildTableSchema {})?
        .add_key_column(timestamp_column_schema)
        .context(BuildTableSchema {})?;

    for col in name_column_map.into_values() {
        schema_builder = schema_builder
            .add_normal_column(col)
            .context(BuildTableSchema {})?;
    }

    schema_builder.build().context(BuildTableSchema {})
}

fn ensure_data_type_compatible(
    table_name: &str,
    column_name: &str,
    is_tag: bool,
    data_type: DatumKind,
    column_schema: &ColumnSchema,
) -> Result<()> {
    ensure!(
        column_schema.is_tag == is_tag,
        InvalidWriteEntry {
            msg: format!(
                "Duplicated column: {column_name} in fields and tags for table: {table_name}",
            ),
        }
    );

    ensure!(
        column_schema.datumKind == data_type,
        InvalidWriteEntry {
            msg: format!(
                "Column: {} in table: {} data type is not same, expected: {}, actual: {}",
                column_name, table_name, column_schema.datumKind, data_type,
            ),
        }
    );

    Ok(())
}

pub fn try_get_data_type_from_value(value: &PbValue) -> Result<DatumKind> {
    match value {
        PbValue::Float64Value(_) => Ok(DatumKind::Double),
        PbValue::StringValue(_) => Ok(DatumKind::String),
        PbValue::Int64Value(_) => Ok(DatumKind::Int64),
        PbValue::Float32Value(_) => Ok(DatumKind::Float),
        PbValue::Int32Value(_) => Ok(DatumKind::Int32),
        PbValue::Int16Value(_) => Ok(DatumKind::Int16),
        PbValue::Int8Value(_) => Ok(DatumKind::Int8),
        PbValue::BoolValue(_) => Ok(DatumKind::Boolean),
        PbValue::Uint64Value(_) => Ok(DatumKind::UInt64),
        PbValue::Uint32Value(_) => Ok(DatumKind::UInt32),
        PbValue::Uint16Value(_) => Ok(DatumKind::UInt16),
        PbValue::Uint8Value(_) => Ok(DatumKind::UInt8),
        PbValue::TimestampValue(_) => Ok(DatumKind::Timestamp),
        PbValue::VarbinaryValue(_) => Ok(DatumKind::Varbinary),
    }
}

/// A planner wraps the datafusion's logical planner, and delegate sql like
/// select/explain to datafusion's planner.
pub(crate) struct PlannerDelegate<'a, P: MetaProvider> {
    metaAndContextProvider: MetaAndContextProvider<'a, P>,
}

impl<'a, P: MetaProvider> PlannerDelegate<'a, P> {
    pub(crate) fn new(metaAndContextProvider: MetaAndContextProvider<'a, P>) -> Self {
        Self { metaAndContextProvider }
    }

    pub(crate) fn sqlStatementToLogicalPlan(self, mut sqlStatement: SqlStatement) -> Result<Plan> {
        match sqlStatement {
            // query statement use data fusion planner
            SqlStatement::Explain { .. } | SqlStatement::Query(_) => {
                normalize_func_name(&mut sqlStatement);
                self.sqlStatement2DatafusionLogicalPlan(sqlStatement)
            }
            SqlStatement::Insert { .. } => self.stmt2InsertPlan(sqlStatement),
            _ => UnsupportedStatement.fail(),
        }
    }

    fn sqlStatement2DatafusionLogicalPlan(self, sqlStatement: SqlStatement) -> Result<Plan> {
        let dataFusionPlanner = SqlToRel::new_with_options(&self.metaAndContextProvider, DEFAULT_PARSER_OPTS);

        let dataFusionLogicalPlan = dataFusionPlanner.sql_statement_to_plan(sqlStatement).context(DatafusionPlan)?;

        debug!("sql statement to data fusion plan, df_plan:\n{:#?}", dataFusionLogicalPlan);

        // Get all tables needed in the plan
        let tableContainer = self.metaAndContextProvider.try_into_container().context(FindMeta)?;

        Ok(Plan::Query(QueryPlan {
            dataFusionLogicalPlan,
            tableContainer: Arc::new(tableContainer),
        }))
    }

    fn buildTsidColumnSchema() -> Result<ColumnSchema> {
        column_schema::Builder::new(TSID_COLUMN.to_string(), DatumKind::UInt64)
            .is_nullable(false).build()
            .context(InvalidColumnSchema { column_name: TSID_COLUMN })
    }

    fn create_table_schema(columns: &[Ident],
                           primary_key_columns: &[Ident],
                           mut columns_by_name: HashMap<&str, ColumnSchema>,
                           column_idxs_by_name: HashMap<&str, usize>) -> Result<Schema> {
        assert_eq!(columns_by_name.len(), column_idxs_by_name.len());

        let mut schema_builder =
            schema::Builder::with_capacity(columns_by_name.len()).auto_increment_column_id(true);

        // Collect the key columns.
        for key_col in primary_key_columns {
            let col_name = key_col.value.as_str();
            let col = columns_by_name
                .remove(col_name)
                .context(UndefinedColumnInPrimaryKey { name: col_name })?;
            schema_builder = schema_builder
                .add_key_column(col)
                .context(BuildTableSchema)?;
        }

        // Collect the normal columns.
        for normal_col in columns {
            let col_name = normal_col.value.as_str();
            // Only normal columns are kept in the `columns_by_name`.
            if let Some(col) = columns_by_name.remove(col_name) {
                schema_builder = schema_builder
                    .add_normal_column(col)
                    .context(BuildTableSchema)?;
            }
        }

        schema_builder.build().context(BuildTableSchema)
    }

    // Find the primary key columns and ensure at most only one exists.
    fn findAndEnsurePrimaryKeyColumns(constraints: &[TableConstraint]) -> Result<Option<Vec<Ident>>> {
        let mut primary_key_columns: Option<Vec<Ident>> = None;
        for constraint in constraints {
            if let TableConstraint::Unique {
                columns,
                is_primary,
                ..
            } = constraint {
                if *is_primary {
                    ensure!(
                        primary_key_columns.is_none(),
                        DuplicatePrimaryKey {
                            first: primary_key_columns.unwrap(),
                            second: columns.clone()
                        }
                    );

                    primary_key_columns = Some(columns.clone());
                }
            }
        }

        Ok(primary_key_columns)
    }

    // Find the timestamp column and ensure its valid existence (only one).
    fn find_and_ensure_timestamp_column(columns_by_name: &HashMap<&str, ColumnSchema>,
                                        constraints: &[TableConstraint]) -> Result<Ident> {
        let mut timestamp_column_name = None;
        for constraint in constraints {
            if let TableConstraint::Unique { columns, .. } = constraint {
                if parser::is_timestamp_key_constraint(constraint) {
                    // Only one timestamp key constraint
                    ensure!(timestamp_column_name.is_none(), InvalidTimestampKey);
                    // Only one column in constraint
                    ensure!(columns.len() == 1, InvalidTimestampKey);
                    let timestamp_ident = columns[0].clone();

                    let timestamp_column = columns_by_name
                        .get(timestamp_ident.value.as_str())
                        .context(TimestampColumnNotFound {
                            name: &timestamp_ident.value,
                        })?;

                    // Ensure the timestamp key's type is timestamp.
                    ensure!(
                        timestamp_column.datumKind == DatumKind::Timestamp,
                        InvalidTimestampKey
                    );
                    // Ensure the timestamp key is not a tag.
                    ensure!(
                        !timestamp_column.is_tag,
                        TimestampKeyTag {
                            name: &timestamp_ident.value,
                        }
                    );

                    timestamp_column_name = Some(timestamp_ident);
                }
            }
        }

        timestamp_column_name.context(RequireTimestamp)
    }

    fn createTableToPlan(&self, stmt: CreateTable) -> Result<Plan> {
        ensure!(!stmt.table_name.is_empty(), CreateTableNameEmpty);

        info!("create table to plan, stmt:{:?}", stmt);

        // Build all column schemas.
        let mut columnName_columnSchema =
            stmt.columns.iter().map(|col| Ok((col.name.value.as_str(), parse_column(col)?)))
                .collect::<Result<HashMap<_, _>>>()?;

        let mut columnName_columnIndex: HashMap<_, _> =
            stmt.columns.iter().enumerate()
                .map(|(idx, col)| (col.name.value.as_str(), idx)).collect();

        // tsid column is a reserved column.
        ensure!(
            !columnName_columnSchema.contains_key(TSID_COLUMN),
            ColumnNameReserved {
                name: TSID_COLUMN.to_string(),
            }
        );

        let timestamp_column =
            Self::find_and_ensure_timestamp_column(&columnName_columnSchema, &stmt.constraints)?;

        let tsid_column = Ident::with_quote(DEFAULT_QUOTE_CHAR, TSID_COLUMN);

        let mut columns: Vec<_> = stmt.columns.iter().map(|col| col.name.clone()).collect();

        let mut addTsidColumnFn = || {
            columnName_columnSchema.insert(TSID_COLUMN, Self::buildTsidColumnSchema()?);
            columnName_columnIndex.insert(TSID_COLUMN, columns.len());
            columns.push(tsid_column.clone());
            Ok(())
        };

        let primaryKeyColumns = match Self::findAndEnsurePrimaryKeyColumns(&stmt.constraints)? {
            Some(primaryKeyColumns) => {
                // Ensure the primary key is defined already.
                for col in &primaryKeyColumns {
                    if &col.value == TSID_COLUMN {
                        // tsid column is a reserved column which can't be defined by user, so let's add it manually.
                        addTsidColumnFn()?;
                    }
                }

                primaryKeyColumns
            }
            None => {
                // No primary key is provided explicitly, so let's use `(tsid,timestamp_key)` as the default primary key.
                addTsidColumnFn()?;

                vec![tsid_column, timestamp_column]
            }
        };

        let table_schema =
            Self::create_table_schema(&columns,
                                      &primaryKeyColumns,
                                      columnName_columnSchema,
                                      columnName_columnIndex, )?;

        let partition_info = match stmt.partition {
            Some(p) => Some(PartitionParser::parse(p)?),
            None => None,
        };

        let options = parse_options(stmt.options)?;

        // ensure default value options are valid
        ensure_column_default_value_valid(table_schema.columns(), &self.metaAndContextProvider)?;

        // TODO: support create table on other catalog/schema
        let table_name = stmt.table_name.to_string();
        let table_ref = get_table_ref(&table_name);

        let createTablePlan = CreateTablePlan {
            engine: stmt.engineName,
            if_not_exists: stmt.if_not_exists,
            tableName: table_ref.table().to_string(),
            table_schema,
            options,
            // TODO: sql parse supports `partition by` syntax.
            partition_info,
        };

        debug!("createTableStatement to createTablePlan, plan:{:?}", createTablePlan);

        Ok(Plan::Create(createTablePlan))
    }

    fn drop_table_to_plan(&self, stmt: DropTable) -> Result<Plan> {
        debug!("Drop table to plan, stmt:{:?}", stmt);

        let (table_name, partition_info) =
            if let Some(table) = self.findTable(&stmt.table_name.to_string())? {
                let table_name = table.name().to_string();
                let partition_info = table.partition_info();
                (table_name, partition_info)
            } else if stmt.if_exists {
                let table_name = stmt.table_name.to_string();
                (table_name, None)
            } else {
                return TableNotFound {
                    name: stmt.table_name.to_string(),
                }
                    .fail();
            };

        let plan = DropTablePlan {
            engine: stmt.engine,
            if_exists: stmt.if_exists,
            table: table_name,
            partition_info,
        };
        debug!("Drop table to plan, plan:{:?}", plan);

        Ok(Plan::Drop(plan))
    }

    fn describe_table_to_plan(&self, stmt: DescribeTable) -> Result<Plan> {
        let table_name = stmt.table_name.to_string();

        let table = self
            .findTable(&table_name)?
            .context(TableNotFound { name: table_name })?;

        Ok(Plan::Describe(DescribeTablePlan { table }))
    }

    // REQUIRE: SqlStatement must be INSERT stmt
    fn stmt2InsertPlan(&self, sqlStatement: SqlStatement) -> Result<Plan> {
        match sqlStatement {
            SqlStatement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                let tableName = TableName::from(table_name).to_string();

                let table = self.findTable(&tableName)?.context(TableNotFound { name: tableName })?;

                let schema = table.schema();

                // Column name and its index in insert stmt: {column name} => index
                let columnName_insertIndex: HashMap<_, _> = columns.iter().enumerate().map(|(idx, ident)| (&ident.value, idx)).collect();
                ensure!(columnName_insertIndex.len() == columns.len(), InsertDuplicateColumns);

                validateInsertStmt(table.name(), &schema, &columnName_insertIndex)?;

                // 以下dataFusion化
                let dataFusionFieldVec = schema.columns().iter()
                    .map(|columnSchema| {
                        DFField::new_unqualified(
                            &columnSchema.name,
                            columnSchema.datumKind.to_arrow_data_type(),
                            columnSchema.is_nullable,
                        )
                    }).collect::<Vec<_>>();

                let dataFusionSchema = DFSchema::new_with_metadata(dataFusionFieldVec, HashMap::new()).context(CreateDatafusionSchema)?;
                let dataFusionPlanner =
                    SqlToRel::new_with_options(&self.metaAndContextProvider,
                                               DEFAULT_PARSER_OPTS);

                // fenquen 要注区分 insertIndex 和column本身index区别 因为 insert时候可能不会涉及全部的column
                // 这个是涉及到表的全部的column的
                let mut insertModeVec = Vec::with_capacity(schema.num_columns());

                // 可能不会涉及到表的全部的column
                let mut columnIndex_defaultValue = BTreeMap::new();

                // Check all not null columns are provided in stmt, also init `column_index_in_insert`
                for (idx, columnSchema) in schema.columns().iter().enumerate() {
                    if let Some(tsid_idx) = schema.tsid_index {
                        if idx == tsid_idx {
                            insertModeVec.push(InsertMode::Auto);
                            continue;
                        }
                    }

                    match columnName_insertIndex.get(&columnSchema.name) {
                        // insert时候涉及到该column
                        Some(insertIndex) => insertModeVec.push(InsertMode::Direct(*insertIndex)),
                        // insert时候未涉及该column
                        None => {
                            // 有defaultValue
                            if let Some(expr) = &columnSchema.default_value {
                                let expr =
                                    dataFusionPlanner.sql_to_expr(expr.clone(),
                                                                  &dataFusionSchema,
                                                                  &mut PlannerContext::new()).context(DatafusionExpr)?;

                                columnIndex_defaultValue.insert(idx, expr);
                                insertModeVec.push(InsertMode::Auto);
                            } else if columnSchema.is_nullable { // nullable
                                insertModeVec.push(InsertMode::Null);
                            } else {
                                return InsertMissingColumn {
                                    table: table.name(),
                                    column: &columnSchema.name,
                                }.fail();
                            }
                        }
                    }
                }

                let rowGroup = build_row_group(schema, source, insertModeVec)?;

                Ok(Plan::Insert(InsertPlan {
                    table,
                    rowGroup,
                    columnIndex_defaultValue,
                }))
            }
            // We already known this stmt is a INSERT stmt
            _ => unreachable!(),
        }
    }

    fn alter_modify_setting_to_plan(&self, stmt: AlterModifySetting) -> Result<Plan> {
        let table_name = stmt.table_name.to_string();

        let table = self
            .findTable(&table_name)?
            .context(TableNotFound { name: table_name })?;
        let plan = AlterTablePlan {
            table,
            operations: AlterTableOperation::ModifySetting(parse_options(stmt.options)?),
        };
        Ok(Plan::AlterTable(plan))
    }

    fn alter_add_column_to_plan(&self, stmt: AlterAddColumn) -> Result<Plan> {
        let table_name = stmt.table_name.to_string();
        let table = self
            .findTable(&table_name)?
            .context(TableNotFound { name: table_name })?;
        let plan = AlterTablePlan {
            table,
            operations: AlterTableOperation::AddColumn(parse_columns(stmt.columns)?),
        };
        Ok(Plan::AlterTable(plan))
    }

    fn exists_table_to_plan(&self, stmt: ExistsTable) -> Result<Plan> {
        let table = self.findTable(&stmt.table_name.to_string())?;
        match table {
            Some(_) => Ok(Plan::Exists(ExistsTablePlan { exists: true })),
            None => Ok(Plan::Exists(ExistsTablePlan { exists: false })),
        }
    }

    fn show_create_to_plan(&self, show_create: ShowCreate) -> Result<Plan> {
        let table_name = show_create.table_name.to_string();
        let table = self
            .findTable(&table_name)?
            .context(TableNotFound { name: table_name })?;
        let plan = ShowCreatePlan {
            table,
            obj_type: show_create.obj_type,
        };
        Ok(Plan::Show(ShowPlan::ShowCreatePlan(plan)))
    }

    fn show_tables_to_plan(&self, show_tables: ShowTables) -> Result<Plan> {
        let plan = ShowTablesPlan {
            pattern: show_tables.pattern,
            query_type: QueryType::Sql,
        };
        Ok(Plan::Show(ShowPlan::ShowTablesPlan(plan)))
    }

    fn show_databases_to_plan(&self) -> Result<Plan> {
        Ok(Plan::Show(ShowPlan::ShowDatabase))
    }

    pub(crate) fn findTable(&self, tableName: &str) -> Result<Option<TableRef>> {
        let table_ref = get_table_ref(tableName);
        self.metaAndContextProvider.table(table_ref).context(MetaProviderFindTable)
    }
}

// Datafusion only support lower-case function name when
// `enable_ident_normalization` is `true`, but we want to
// function case-insensitive, so add this normalization.
fn normalize_func_name(sql_stmt: &mut SqlStatement) {
    let normalize_expr = |expr: &mut SqlExpr| {
        if let SqlExpr::Function(ref mut func) = expr {
            for ident in &mut func.name.0 {
                ident.value = ident.value.to_lowercase();
            }
        }
    };

    visit_statements_mut(sql_stmt, |stmt| {
        if let SqlStatement::Query(q) = stmt {
            if let SetExpr::Select(select) = q.body.as_mut() {
                for projection in &mut select.projection {
                    match projection {
                        SelectItem::UnnamedExpr(ref mut expr) => normalize_expr(expr),
                        SelectItem::ExprWithAlias { ref mut expr, .. } => normalize_expr(expr),
                        SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => {}
                    }
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

#[derive(Debug)]
enum InsertMode {
    /// 在insert时候涉及到了该column
    Direct(usize),
    /// 在insert时候未涉及到该column 且该column没有默认值且是nullable
    Null,
    /// 在insert时候未涉及到该column 且该column是有默认值的or是tsid到后来自动生成
    Auto,
}

fn parseDataValFromExpr(datumKind: DatumKind, expr: &mut Expr) -> Result<Datum> {
    match expr {
        Expr::Value(value) => Datum::fromValue(&datumKind, mem::replace(value, Value::Null)).context(InsertConvertValue),
        Expr::UnaryOp { op: unaryOperator, expr: child_expr } => {
            let negative = match unaryOperator {
                UnaryOperator::Minus => true,
                UnaryOperator::Plus => false,
                _ => InsertExprNotValue { source_expr: Expr::UnaryOp { op: *unaryOperator, expr: child_expr.clone() } }.fail()?,
            };

            let mut datum = parseDataValFromExpr(datumKind, child_expr)?;

            if negative {
                datum = datum.to_negative().with_context(|| InsertExprNoNegativeValue { source_expr: expr.clone() })?;
            }

            Ok(datum)
        }
        _ => InsertExprNotValue { source_expr: expr.clone() }.fail(),
    }
}

fn build_row_group(schema: Schema,
                   source: Box<Query>,
                   insertModeVec: Vec<InsertMode>) -> Result<RowGroup> {
    // Build row group by schema
    match *source.body {
        SetExpr::Values(Values {
                            explicit_row: _,
                            rows,
                        }) => {
            let mut rowGroupBuilder = RowGroupBuilder::with_capacity(schema.clone(), rows.len());
            for mut row in rows {
                // build row
                let mut row_builder = rowGroupBuilder.row_builder();

                // for each column in schema, append datum into row builder
                for (insertMode, columnSchema) in insertModeVec.iter().zip(schema.columns()) {
                    match insertMode {
                        InsertMode::Direct(index) => {
                            let exprs_len = row.len();
                            let expr = row.get_mut(*index).context(InsertValuesNotEnough {
                                len: exprs_len,
                                index: *index,
                            })?;

                            let datum = parseDataValFromExpr(columnSchema.datumKind, expr)?;
                            row_builder = row_builder.append_datum(datum).context(BuildRow)?;
                        }
                        // null column
                        InsertMode::Null => row_builder = row_builder.append_datum(Datum::Null).context(BuildRow)?,
                        // 先暂时用各个类型的0值来填充
                        InsertMode::Auto => row_builder = row_builder.append_datum(Datum::empty(&columnSchema.datumKind)).context(BuildRow)?,
                    }
                }

                // finish this row and append into row group
                row_builder.finish().context(BuildRow)?;
            }

            // Build the whole row group
            Ok(rowGroupBuilder.build())
        }
        _ => InsertSourceBodyNotSet.fail(),
    }
}

#[inline]
fn is_tsid_column(name: &str) -> bool {
    name == TSID_COLUMN
}

// 确保用户是不能自己单独专门insert tsid这个字段的 fenquen
fn validateInsertStmt(table_name: &str,
                      schema: &Schema,
                      columnName_columnIndex: &HashMap<&String, usize>) -> Result<()> {
    for name in columnName_columnIndex.keys() {
        if is_tsid_column(name.as_str()) {
            return Err(Error::InsertReservedColumn {
                table: table_name.to_string(),
                column: name.to_string(),
            });
        }

        schema.column_with_name(name).context(UnknownInsertColumn { name: name.to_string() })?;
    }

    Ok(())
}

fn parse_options(options: Vec<SqlOption>) -> Result<HashMap<String, String>> {
    let mut parsed_options = HashMap::with_capacity(options.len());

    for option in options {
        let key = option.name.value;
        if let Some(value) = parse_for_option(option.value)? {
            parsed_options.insert(key, value);
        };
    }

    Ok(parsed_options)
}

/// Parse value for sql option.
pub fn parse_for_option(value: Value) -> Result<Option<String>> {
    let value_opt = match value {
        Value::Number(n, _long) => Some(n),
        Value::SingleQuotedString(v) | Value::DoubleQuotedString(v) | Value::UnQuotedString(v) => {
            Some(v)
        }
        Value::NationalStringLiteral(v) | Value::HexStringLiteral(v) => {
            return UnsupportedOption { value: v }.fail();
        }
        Value::Boolean(v) => Some(v.to_string()),
        // Ignore this option if value is null.
        Value::Null
        | Value::Placeholder(_)
        | Value::EscapedStringLiteral(_)
        | Value::SingleQuotedByteStringLiteral(_)
        | Value::DoubleQuotedByteStringLiteral(_)
        | Value::RawStringLiteral(_)
        | Value::DollarQuotedString(_) => None,
    };

    Ok(value_opt)
}

fn parse_columns(cols: Vec<ColumnDef>) -> Result<Vec<ColumnSchema>> {
    let mut parsed_columns = Vec::with_capacity(cols.len());

    // Build all column schemas.
    for col in &cols {
        parsed_columns.push(parse_column(col)?);
    }

    Ok(parsed_columns)
}

fn parse_column(col: &ColumnDef) -> Result<ColumnSchema> {
    let mut data_type = DatumKind::try_from(&col.data_type).context(UnsupportedDataType)?;

    // Process column options
    let mut is_nullable = true; // A column is nullable by default.
    let mut is_tag = false;
    let mut is_dictionary = false;
    let mut is_unsign = false;
    let mut comment = String::new();
    let mut default_value = None;
    for option_def in &col.options {
        if matches!(option_def.option, ColumnOption::NotNull) {
            is_nullable = false;
        } else if parser::is_tag_column(&option_def.option) {
            is_tag = true;
        } else if parser::is_dictionary_column(&option_def.option) {
            is_dictionary = true;
        } else if parser::is_unsign_column(&option_def.option) {
            is_unsign = true;
        } else if let Some(default_value_expr) = parser::get_default_value(&option_def.option) {
            default_value = Some(default_value_expr);
        } else if let Some(v) = parser::get_column_comment(&option_def.option) {
            comment = v;
        }
    }

    if is_unsign {
        data_type = data_type
            .unsign_kind()
            .context(InvalidUnsignType { kind: data_type })?;
    }

    let builder = column_schema::Builder::new(col.name.value.clone(), data_type)
        .is_nullable(is_nullable)
        .is_tag(is_tag)
        .is_dictionary(is_dictionary)
        .comment(comment)
        .default_value(default_value);

    builder.build().context(InvalidColumnSchema {
        column_name: &col.name.value,
    })
}

// Ensure default value option of columns are valid.
fn ensure_column_default_value_valid<P: MetaProvider>(
    columns: &[ColumnSchema],
    meta_provider: &MetaAndContextProvider<'_, P>,
) -> Result<()> {
    let df_planner = SqlToRel::new_with_options(meta_provider, DEFAULT_PARSER_OPTS);
    let mut df_schema = DFSchema::empty();
    let mut arrow_schema = ArrowSchema::empty();

    for column_def in columns.iter() {
        if let Some(expr) = &column_def.default_value {
            let df_logical_expr = df_planner
                .sql_to_expr(expr.clone(), &df_schema, &mut PlannerContext::new())
                .context(DatafusionExpr)?;

            // Create physical expr
            let execution_props = ExecutionProps::default();
            let df_schema_ref = Arc::new(df_schema.clone());
            let simplifier = ExprSimplifier::new(
                SimplifyContext::new(&execution_props).with_schema(df_schema_ref.clone()),
            );
            let df_logical_expr = simplifier
                .coerce(df_logical_expr, df_schema_ref.clone())
                .context(DatafusionExpr)?;

            let df_logical_expr = simplifier
                .simplify(df_logical_expr)
                .context(DatafusionExpr)?;

            let physical_expr = create_physical_expr(
                &df_logical_expr,
                &df_schema,
                &arrow_schema,
                &execution_props,
            )
                .context(DatafusionExpr)?;

            let from_type = physical_expr
                .data_type(&arrow_schema)
                .context(DatafusionDataType)?;
            ensure! {
                can_cast_types(&from_type, &column_def.datumKind.into()),
                InvalidDefaultValueCoercion::<Expr, ArrowDataType, ArrowDataType>{
                    expr: expr.clone(),
                    from: from_type,
                    to: column_def.datumKind.into(),
                },
            }
        }

        // Add evaluated column to schema
        let new_arrow_field = ArrowField::try_from(column_def).unwrap();
        let to_merged_df_schema = &DFSchema::new_with_metadata(
            vec![DFField::from(new_arrow_field.clone())],
            HashMap::new(),
        )
            .context(CreateDatafusionSchema)?;
        df_schema.merge(to_merged_df_schema);
        arrow_schema =
            ArrowSchema::try_merge(vec![arrow_schema, ArrowSchema::new(vec![new_arrow_field])])
                .context(MergeArrowSchema)?;
    }

    Ok(())
}

// Workaround for TableReference::from(&str)
// it will always convert table to lowercase when not quoted
// TODO: support catalog/schema
pub fn get_table_ref(tableName: &str) -> TableReference {
    TableReference::from(ResolvedTableReference {
        catalog: Cow::from(DEFAULT_CATALOG),
        schema: Cow::from(DEFAULT_SCHEMA),
        table: Cow::from(tableName),
    })
}