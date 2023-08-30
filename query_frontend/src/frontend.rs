// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Frontend

use std::{sync::Arc, time::Instant};

use ceresdbproto::{prometheus::Expr as PromExpr, storage::WriteTableRequest};
use cluster::config::SchemaConfig;
use common_types::request_id::RequestId;
use generic_error::GenericError;
use influxql_parser::statement::Statement as InfluxqlStatement;
use macros::define_result;
use prom_remote_api::types::Query as PromRemoteQuery;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use sqlparser::ast::{SetExpr, Statement as SqlStatement, TableFactor};
use table_engine::table;

use crate::{
    ast::{Statement, TableName},
    parser::Parser,
    plan::Plan,
    planner::Planner,
    promql::{ColumnNames, Expr, RemoteQueryPlan},
    provider::MetaProvider,
};

#[derive(Debug, Snafu)]
pub enum Error {
    // Invalid sql is quite common, so we don't provide a backtrace now.
    #[snafu(display("Invalid sql, sql:{}, err:{}", sql, source))]
    InvalidSql {
        sql: String,
        source: sqlparser::parser::ParserError,
    },

    // TODO(yingwen): Should we store stmt here?
    #[snafu(display("Failed to create plan, err:{}", source))]
    CreatePlan { source: crate::planner::Error },

    #[snafu(display("Invalid prom request, err:{}", source))]
    InvalidPromRequest { source: crate::promql::Error },

    #[snafu(display("Expr is not found in prom request.\nBacktrace:\n{}", backtrace))]
    ExprNotFoundInPromRequest { backtrace: Backtrace },

    // invalid sql is quite common, so we don't provide a backtrace now.
    #[snafu(display("invalid influxql, influxql:{}, err:{}", influxql, parse_err))]
    InvalidInfluxql {
        influxql: String,
        parse_err: influxql_parser::common::ParseError,
    },

    #[snafu(display("Failed to build influxql plan, msg:{}, err:{}", msg, source))]
    InfluxqlPlanWithCause { msg: String, source: GenericError },

    #[snafu(display("Failed to build influxql plan, msg:{}", msg))]
    InfluxqlPlan { msg: String },
}

define_result!(Error);

pub type StatementVec = Vec<Statement>;

/// Context used by Frontend
///
/// We can collect metrics and trace info in it instead of using global
/// metrics or trace collector.
pub struct Context {
    /// Id of the query request.
    pub request_id: RequestId,
    /// Parallelism to read table.
    pub read_parallelism: usize,
    /// Deadline of this request
    pub deadline: Option<Instant>,
}

impl Context {
    pub fn new(request_id: RequestId, deadline: Option<Instant>) -> Self {
        Self {
            request_id,
            deadline,
            read_parallelism: table::DEFAULT_READ_PARALLELISM,
        }
    }
}

/// SQL frontend implementation
///
/// Thought the parser supports using multiple statements in a sql, but
/// this frontend only support planning one statement at a time now
#[derive(Debug)]
pub struct Frontend<P> {
    provider: P,
}

impl<P> Frontend<P> {
    pub fn new(provider: P) -> Self {
        Self { provider }
    }

    pub fn parse_sql(&self, _ctx: &mut Context, sql: &str) -> Result<StatementVec> {
        Parser::parse_sql(sql).context(InvalidSql { sql })
    }

    /// Parse the request and returns the Expr
    pub fn parse_promql(&self, _ctx: &mut Context, expr: Option<PromExpr>) -> Result<Expr> {
        let expr = expr.context(ExprNotFoundInPromRequest)?;
        Expr::try_from(expr).context(InvalidPromRequest)
    }

    /// Parse the sql and returns the statements
    pub fn parse_influxql(&self,
                          _ctx: &mut Context,
                          influxql: &str) -> Result<Vec<InfluxqlStatement>> {
        match influxql_parser::parse_statements(influxql) {
            Ok(statementVec) => Ok(statementVec),
            Err(e) => Err(Error::InvalidInfluxql {
                influxql: influxql.to_string(),
                parse_err: e,
            }),
        }
    }
}

impl<P: MetaProvider> Frontend<P> {
    /// create logical plan for the statement
    pub fn statementToPlan(&self, ctx: &mut Context, statement: Statement) -> Result<Plan> {
        let planner = Planner::new(&self.provider, ctx.request_id, ctx.read_parallelism);

        planner.statementToPlan(statement).context(CreatePlan)
    }

    /// Experimental native promql support, not used in production yet.
    pub fn promql_expr_to_plan(&self,
                               ctx: &mut Context,
                               expr: Expr) -> Result<(Plan, Arc<ColumnNames>)> {
        let planner = Planner::new(&self.provider, ctx.request_id, ctx.read_parallelism);

        planner.promql_expr_to_plan(expr).context(CreatePlan)
    }

    /// Prometheus remote query support
    pub fn prom_remote_query_to_plan(
        &self,
        ctx: &mut Context,
        query: PromRemoteQuery,
    ) -> Result<RemoteQueryPlan> {
        let planner = Planner::new(&self.provider, ctx.request_id, ctx.read_parallelism);
        planner.remote_prom_req_to_plan(query).context(CreatePlan)
    }

    pub fn influxql_stmt_to_plan(
        &self,
        ctx: &mut Context,
        stmt: InfluxqlStatement,
    ) -> Result<Plan> {
        let planner = Planner::new(&self.provider, ctx.request_id, ctx.read_parallelism);
        planner.influxql_stmt_to_plan(stmt).context(CreatePlan)
    }

    pub fn write_req_to_plan(
        &self,
        ctx: &mut Context,
        schema_config: &SchemaConfig,
        write_table: &WriteTableRequest,
    ) -> Result<Plan> {
        let planner = Planner::new(&self.provider, ctx.request_id, ctx.read_parallelism);

        planner
            .write_req_to_plan(schema_config, write_table)
            .context(CreatePlan)
    }
}

/// statementVec只会有1个元素的
pub fn parse_table_name(statementVec: &StatementVec) -> Option<String> {
    // maybe have empty sql
    if statementVec.is_empty() {
        return None;
    }

    match &statementVec[0] {
        Statement::Standard(s) => match *s.clone() {
            SqlStatement::Insert { table_name, .. } => {
                Some(TableName::from(table_name).to_string())
            }
            SqlStatement::Explain { statement, .. } => {
                if let SqlStatement::Query(q) = *statement {
                    match *q.body {
                        SetExpr::Select(select) => {
                            if select.from.len() != 1 {
                                None
                            } else if let TableFactor::Table { name, .. } = &select.from[0].relation {
                                Some(TableName::from(name.clone()).to_string())
                            } else {
                                None
                            }
                        }
                        // TODO: return unsupported error rather than none.
                        _ => None,
                    }
                } else {
                    None
                }
            }
            SqlStatement::Query(q) => match *q.body {
                SetExpr::Select(select) => {
                    if select.from.len() != 1 {
                        None
                    } else if let TableFactor::Table { name, .. } = &select.from[0].relation {
                        Some(TableName::from(name.clone()).to_string())
                    } else {
                        None
                    }
                }
                _ => None,
            },
            _ => None,
        },
        Statement::Create(s) => Some(s.table_name.to_string()),
        Statement::Drop(s) => Some(s.table_name.to_string()),
        Statement::Describe(s) => Some(s.table_name.to_string()),
        Statement::AlterModifySetting(s) => Some(s.table_name.to_string()),
        Statement::AlterAddColumn(s) => Some(s.table_name.to_string()),
        Statement::ShowCreate(s) => Some(s.table_name.to_string()),
        Statement::ShowTables(_s) => None,
        Statement::ShowDatabases => None,
        Statement::Exists(s) => Some(s.table_name.to_string()),
    }
}

pub fn parse_table_name_with_sql(sql: &str) -> Result<Option<String>> {
    Ok(parse_table_name(
        &Parser::parse_sql(sql).context(InvalidSql { sql })?,
    ))
}