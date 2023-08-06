// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{mem, sync::Arc};

use arrow::{compute, compute::kernels::cast_utils::string_to_timestamp_nanos, error::ArrowError};
use chrono::{Local, LocalResult, NaiveDateTime, TimeZone, Utc};
use datafusion::{
    arrow::datatypes::DataType,
    common::{
        tree_node::{TreeNode, TreeNodeRewriter},
        DFSchemaRef,
    },
    config::ConfigOptions,
    error::{DataFusionError, Result},
    logical_expr::{
        expr::{Expr, InList},
        logical_plan::{Filter, LogicalPlan, TableScan},
        utils, Between, BinaryExpr, ExprSchemable, Operator,
    },
    optimizer::analyzer::AnalyzerRule,
    scalar::ScalarValue,
};
use log::debug;

/// Optimizer that cast literal value to target column's type
///
/// Example transformations that are applied:
/// * `expr > '5'` to `expr > 5` when `expr` is of numeric type
/// * `expr > '2021-12-02 15:00:34'` to `expr > 1638428434000(ms)` when `expr`
///   is of timestamp type
/// * `expr > 10` to `expr > '10'` when `expr` is of string type
/// * `expr = 'true'` to `expr = true` when `expr` is of boolean type
pub struct TypeConversion;

impl AnalyzerRule for TypeConversion {
    #[allow(clippy::only_used_in_recursion)]
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        #[allow(deprecated)]
        let mut rewriter = TypeRewriter {
            schemas: plan.all_schemas(),
        };

        match &plan {
            LogicalPlan::Filter(Filter {
                predicate, input, ..
            }) => {
                let input: &LogicalPlan = input;
                let predicate = predicate.clone().rewrite(&mut rewriter)?;
                let input = self.analyze(input.clone(), config)?;
                Ok(LogicalPlan::Filter(Filter::try_new(
                    predicate,
                    Arc::new(input),
                )?))
            }
            LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                projection,
                projected_schema,
                filters,
                fetch,
            }) => {
                let rewrite_filters = filters
                    .clone()
                    .into_iter()
                    .map(|e| e.rewrite(&mut rewriter))
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::TableScan(TableScan {
                    table_name: table_name.clone(),
                    source: source.clone(),
                    projection: projection.clone(),
                    projected_schema: projected_schema.clone(),
                    filters: rewrite_filters,
                    fetch: *fetch,
                }))
            }
            LogicalPlan::Projection { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Repartition { .. }
            | LogicalPlan::Extension { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Union { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::CrossJoin { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::Analyze { .. }
            | LogicalPlan::Distinct { .. }
            | LogicalPlan::Prepare { .. }
            | LogicalPlan::DescribeTable { .. }
            | LogicalPlan::Ddl { .. }
            | LogicalPlan::Dml { .. } => {
                let inputs = plan.inputs();
                let new_inputs = inputs
                    .into_iter()
                    .map(|plan| self.analyze(plan.clone(), config))
                    .collect::<Result<Vec<_>>>()?;

                let expr = plan
                    .expressions()
                    .into_iter()
                    .map(|e| e.rewrite(&mut rewriter))
                    .collect::<Result<Vec<_>>>()?;

                Ok(utils::from_plan(&plan, &expr, &new_inputs)?)
            }
            LogicalPlan::Subquery(_)
            | LogicalPlan::Statement { .. }
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Unnest(_)
            | LogicalPlan::EmptyRelation { .. } => Ok(plan.clone()),
        }
    }

    fn name(&self) -> &str {
        "ceresdb_type_conversion"
    }
}

struct TypeRewriter<'a> {
    /// input schemas
    schemas: Vec<&'a DFSchemaRef>,
}

impl<'a> TypeRewriter<'a> {
    fn column_data_type(&self, expr: &Expr) -> Option<DataType> {
        if let Expr::Column(_) = expr {
            for schema in &self.schemas {
                if let Ok(v) = expr.get_type(schema) {
                    return Some(v);
                }
            }
        }

        None
    }

    fn convert_type<'b>(&self, mut left: &'b Expr, mut right: &'b Expr) -> Result<(Expr, Expr)> {
        let left_type = self.column_data_type(left);
        let right_type = self.column_data_type(right);

        let mut reverse = false;
        let left_type = match (&left_type, &right_type) {
            (Some(v), None) => v,
            (None, Some(v)) => {
                reverse = true;
                mem::swap(&mut left, &mut right);
                v
            }
            _ => return Ok((left.clone(), right.clone())),
        };

        match (left, right) {
            (Expr::Column(col), Expr::Literal(value)) => {
                let casted_right = Self::cast_scalar_value(value, left_type)?;
                debug!(
                    "TypeRewriter convert type, origin_left:{:?}, type:{}, right:{:?}, casted_right:{:?}",
                    col, left_type, value, casted_right
                );
                if casted_right.is_null() {
                    return Err(DataFusionError::Plan(format!(
                        "column:{col:?} value:{value:?} is invalid"
                    )));
                }
                if reverse {
                    Ok((Expr::Literal(casted_right), left.clone()))
                } else {
                    Ok((left.clone(), Expr::Literal(casted_right)))
                }
            }
            _ => Ok((left.clone(), right.clone())),
        }
    }

    fn cast_scalar_value(value: &ScalarValue, data_type: &DataType) -> Result<ScalarValue> {
        if let DataType::Timestamp(_, _) = data_type {
            if let ScalarValue::Utf8(Some(v)) = value {
                return match string_to_timestamp_ms_workaround(v) {
                    Ok(v) => Ok(v),
                    _ => string_to_timestamp_ms(v),
                };
            }
        }

        if let DataType::Boolean = data_type {
            if let ScalarValue::Utf8(Some(v)) = value {
                return match v.to_lowercase().as_str() {
                    "true" => Ok(ScalarValue::Boolean(Some(true))),
                    "false" => Ok(ScalarValue::Boolean(Some(false))),
                    _ => Ok(ScalarValue::Boolean(None)),
                };
            }
        }

        let array = value.to_array();
        ScalarValue::try_from_array(
            &compute::cast(&array, data_type).map_err(DataFusionError::ArrowError)?,
            // index: Converts a value in `array` at `index` into a ScalarValue
            0,
        )
    }
}

impl<'a> TreeNodeRewriter for TypeRewriter<'a> {
    type N = Expr;

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        let new_expr = match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => {
                    let (left, right) = self.convert_type(&left, &right)?;
                    Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(left),
                        op,
                        right: Box::new(right),
                    })
                }
                _ => Expr::BinaryExpr(BinaryExpr { left, op, right }),
            },
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                let (expr, low) = self.convert_type(&expr, &low)?;
                let (expr, high) = self.convert_type(&expr, &high)?;
                Expr::Between(Between {
                    expr: Box::new(expr),
                    negated,
                    low: Box::new(low),
                    high: Box::new(high),
                })
            }
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                let mut list_expr = Vec::with_capacity(list.len());
                for e in list {
                    let (_, expr_conversion) = self.convert_type(&expr, &e)?;
                    list_expr.push(expr_conversion);
                }
                Expr::InList(InList {
                    expr,
                    list: list_expr,
                    negated,
                })
            }
            Expr::Literal(value) => match value {
                ScalarValue::TimestampSecond(Some(i), _) => {
                    timestamp_to_timestamp_ms_expr(TimestampType::Second, i)
                }
                ScalarValue::TimestampMicrosecond(Some(i), _) => {
                    timestamp_to_timestamp_ms_expr(TimestampType::Microsecond, i)
                }
                ScalarValue::TimestampNanosecond(Some(i), _) => {
                    timestamp_to_timestamp_ms_expr(TimestampType::Nanosecond, i)
                }
                _ => Expr::Literal(value),
            },
            expr => {
                // no rewrite possible
                expr
            }
        };
        Ok(new_expr)
    }
}

fn string_to_timestamp_ms(string: &str) -> Result<ScalarValue> {
    let ts = string_to_timestamp_nanos(string)
        .map(|t| t / 1_000_000)
        .map_err(DataFusionError::from)?;
    Ok(ScalarValue::TimestampMillisecond(Some(ts), None))
}

// TODO(lee): remove following codes after PR(https://github.com/apache/arrow-rs/pull/3787) merged
fn string_to_timestamp_ms_workaround(string: &str) -> Result<ScalarValue> {
    // Because function `string_to_timestamp_nanos` returns a NaiveDateTime's
    // nanoseconds from a string without a specify time zone, We need to convert
    // it to local timestamp.

    // without a timezone specifier as a local time, using 'T' as a separator
    // Example: 2020-09-08T13:42:29.190855
    if let Ok(ts) = NaiveDateTime::parse_from_str(string, "%Y-%m-%dT%H:%M:%S%.f") {
        let mills = naive_datetime_to_timestamp(string, ts).map_err(DataFusionError::from)?;
        return Ok(ScalarValue::TimestampMillisecond(Some(mills), None));
    }

    // without a timezone specifier as a local time, using ' ' as a separator
    // Example: 2020-09-08 13:42:29.190855
    if let Ok(ts) = NaiveDateTime::parse_from_str(string, "%Y-%m-%d %H:%M:%S%.f") {
        let mills = naive_datetime_to_timestamp(string, ts).map_err(DataFusionError::from)?;
        return Ok(ScalarValue::TimestampMillisecond(Some(mills), None));
    }

    Err(ArrowError::CastError(format!(
        "Error parsing '{string}' as timestamp: local time representation is invalid"
    )))
    .map_err(DataFusionError::from)
}

/// Converts the naive datetime (which has no specific timezone) to a
/// nanosecond epoch timestamp relative to UTC.
/// copy from:https://github.com/apache/arrow-rs/blob/6a6e7f72331aa6589aa676577571ffed98d52394/arrow/src/compute/kernels/cast_utils.rs#L208
fn naive_datetime_to_timestamp(s: &str, datetime: NaiveDateTime) -> Result<i64, ArrowError> {
    let l = Local {};

    match l.from_local_datetime(&datetime) {
        LocalResult::None => Err(ArrowError::CastError(format!(
            "Error parsing '{s}' as timestamp: local time representation is invalid"
        ))),
        LocalResult::Single(local_datetime) => {
            Ok(local_datetime.with_timezone(&Utc).timestamp_nanos() / 1_000_000)
        }

        LocalResult::Ambiguous(local_datetime, _) => {
            Ok(local_datetime.with_timezone(&Utc).timestamp_nanos() / 1_000_000)
        }
    }
}

enum TimestampType {
    Second,
    #[allow(dead_code)]
    Millisecond,
    Microsecond,
    Nanosecond,
}

fn timestamp_to_timestamp_ms_expr(typ: TimestampType, timestamp: i64) -> Expr {
    let timestamp = match typ {
        TimestampType::Second => timestamp * 1_000,
        TimestampType::Millisecond => timestamp,
        TimestampType::Microsecond => timestamp / 1_000,
        TimestampType::Nanosecond => timestamp / 1_000 / 1_000,
    };

    Expr::Literal(ScalarValue::TimestampMillisecond(Some(timestamp), None))
}