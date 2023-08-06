// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! This module convert Prometheus remote query to datafusion plan.

use std::sync::Arc;

use common_types::{schema::Schema, time::TimeRange};
use datafusion::{
    logical_expr::LogicalPlanBuilder,
    optimizer::utils::conjunction,
    prelude::{ident, lit, regexp_match, Expr},
    sql::{planner::ContextProvider, TableReference},
};
use prom_remote_api::types::{label_matcher, LabelMatcher, Query};
use snafu::{OptionExt, ResultExt};

use crate::{
    plan::{Plan, QueryPlan},
    promql::{
        convert::Selector,
        datafusion_util::{default_sort_exprs, timerange_to_expr},
        error::*,
    },
    provider::{ContextProviderAdapter, MetaProvider},
};

pub const NAME_LABEL: &str = "__name__";
pub const DEFAULT_FIELD_COLUMN: &str = "value";
const FIELD_LABEL: &str = "__ceresdb_field__";

pub struct RemoteQueryPlan {
    pub plan: Plan,
    pub field_col_name: String,
    pub timestamp_col_name: String,
}
/// Generate a plan like this
/// ```plaintext
/// Sort: (tsid, timestamp) asc
///   Project:
///     Filter:
///       TableScan
/// ```
pub fn remote_query_to_plan<P: MetaProvider>(
    query: Query,
    meta_provider: ContextProviderAdapter<'_, P>,
) -> Result<RemoteQueryPlan> {
    let (metric, field, mut filters) = normalize_matchers(query.matchers)?;

    let table_provider = meta_provider
        .get_table_provider(TableReference::bare(&metric))
        .context(TableProviderNotFound { name: &metric })?;
    let schema = Schema::try_from(table_provider.schema()).context(BuildTableSchema)?;
    let timestamp_col_name = schema.timestamp_name();

    // Build datafusion plan
    let filter_exprs = {
        let query_range = TimeRange::new_unchecked(
            query.start_timestamp_ms.into(),
            (query.end_timestamp_ms + 1).into(), // end is inclusive
        );
        filters.push(timerange_to_expr(query_range, timestamp_col_name));
        conjunction(filters).expect("at least one filter(timestamp)")
    };
    let (projection_exprs, _) = Selector::build_projection_tag_keys(&schema, &field)?;
    let sort_exprs = default_sort_exprs(timestamp_col_name);
    let df_plan = LogicalPlanBuilder::scan(metric.clone(), table_provider, None)?
        .filter(filter_exprs)?
        .project(projection_exprs)?
        .sort(sort_exprs)?
        .build()
        .context(BuildPlanError)?;

    let tables = Arc::new(
        meta_provider
            .try_into_container()
            .context(MetaProviderError {
                msg: "Failed to find meta",
            })?,
    );
    Ok(RemoteQueryPlan {
        plan: Plan::Query(QueryPlan { dataFusionLogicalPlan: df_plan, tables }),
        field_col_name: field,
        timestamp_col_name: timestamp_col_name.to_string(),
    })
}

/// Extract metric, field from matchers, and convert remaining matchers to
/// datafusion exprs
fn normalize_matchers(matchers: Vec<LabelMatcher>) -> Result<(String, String, Vec<Expr>)> {
    let mut metric = None;
    let mut field = None;
    let mut filters = Vec::with_capacity(matchers.len());
    for m in matchers {
        match m.name.as_str() {
            NAME_LABEL => metric = Some(m.value),
            FIELD_LABEL => field = Some(m.value),
            _ => {
                let col_name = ident(&m.name);
                let expr = match m.r#type() {
                    label_matcher::Type::Eq => col_name.eq(lit(m.value)),
                    label_matcher::Type::Neq => col_name.not_eq(lit(m.value)),
                    // https://github.com/prometheus/prometheus/blob/2ce94ac19673a3f7faf164e9e078a79d4d52b767/model/labels/regexp.go#L29
                    label_matcher::Type::Re => {
                        regexp_match(vec![col_name, lit(format!("^(?:{})$", m.value))])
                            .is_not_null()
                    }
                    label_matcher::Type::Nre => {
                        regexp_match(vec![col_name, lit(format!("^(?:{})$", m.value))]).is_null()
                    }
                };

                filters.push(expr);
            }
        }
    }

    Ok((
        metric.context(InvalidExpr {
            msg: "Metric not found",
        })?,
        field.unwrap_or_else(|| DEFAULT_FIELD_COLUMN.to_string()),
        filters,
    ))
}