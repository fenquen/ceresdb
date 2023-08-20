// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Helper function and struct to find input columns for an Expr;

use datafusion::logical_expr::expr::Expr;

pub fn find_columns_by_expr(expr: &Expr) -> Vec<String> {
    expr.to_columns().unwrap().into_iter().map(|col| col.name).collect()
}
