// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::Column,
    logical_expr::{expr::InList, Expr, Operator},
    scalar::ScalarValue,
};

const MAX_ELEMS_IN_LIST_FOR_FILTER: usize = 100;

/// A position used to describe the location of a column in the row groups.
#[derive(Debug, Clone, Copy)]
pub struct ColumnPosition {
    pub row_group_idx: usize,
    pub column_idx: usize,
}

/// Filter the row groups according to the `exprs`.
///
/// The return value is the filtered row group indexes. And the `is_equal`
/// closure receive three parameters:
/// - The position of the column in the row groups;
/// - The value of the column used to determine equality;
/// - Whether this compare is negated;
/// And it should return the result of this comparison, and None denotes
/// unknown.
pub fn prune_row_groups<E>(
    schema: SchemaRef,
    exprs: &[Expr],
    num_row_groups: usize,
    is_equal: E,
) -> Vec<usize>
where
    E: Fn(ColumnPosition, &ScalarValue, bool) -> Option<bool>,
{
    let mut should_reads = vec![true; num_row_groups];
    for expr in exprs {
        let pruner = EqPruner::new(expr);
        for (row_group_idx, should_read) in should_reads.iter_mut().enumerate() {
            if !*should_read {
                continue;
            }

            let f = |column: &Column, val: &ScalarValue, negated: bool| -> bool {
                match schema.column_with_name(&column.name) {
                    Some((column_idx, _)) => {
                        let pos = ColumnPosition {
                            row_group_idx,
                            column_idx,
                        };
                        // Just set the result is true to ensure not to miss any possible row group
                        // if the caller has no idea of the compare result.
                        is_equal(pos, val, negated).unwrap_or(true)
                    }
                    _ => true,
                }
            };

            *should_read = pruner.prune(&f);
        }
    }

    should_reads
        .iter()
        .enumerate()
        .filter_map(|(row_group_idx, should_read)| {
            if *should_read {
                Some(row_group_idx)
            } else {
                None
            }
        })
        .collect()
}

/// A pruner based on (not)equal predicates, including in-list predicate.
#[derive(Debug, Clone)]
pub struct EqPruner {
    /// Normalized expression for pruning.
    normalized_expr: NormalizedExpr,
}

impl EqPruner {
    pub fn new(predicate_expr: &Expr) -> Self {
        Self {
            normalized_expr: normalize_predicate_expression(predicate_expr),
        }
    }

    /// Use the prune function provided by caller to finish pruning.
    ///
    /// The prune function receives three parameters:
    /// - the column to compare;
    /// - the value of the column used to determine equality;
    /// - Whether this compare is negated;
    pub fn prune<F>(&self, f: &F) -> bool
    where
        F: Fn(&Column, &ScalarValue, bool) -> bool,
    {
        self.normalized_expr.compute(f)
    }
}

/// The normalized expression based on [`datafusion::logical_expr::Expr`].
///
/// It only includes these kinds of `And`, `Or`, `Eq`, `NotEq` and `True`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum NormalizedExpr {
    And {
        left: Box<NormalizedExpr>,
        right: Box<NormalizedExpr>,
    },
    Or {
        left: Box<NormalizedExpr>,
        right: Box<NormalizedExpr>,
    },
    Eq {
        column: Column,
        value: ScalarValue,
    },
    NotEq {
        column: Column,
        value: ScalarValue,
    },
    True,
    False,
}

impl NormalizedExpr {
    fn boxed(self) -> Box<Self> {
        Box::new(self)
    }

    fn compute<F>(&self, f: &F) -> bool
    where
        F: Fn(&Column, &ScalarValue, bool) -> bool,
    {
        match self {
            NormalizedExpr::And { left, right } => left.compute(f) && right.compute(f),
            NormalizedExpr::Or { left, right } => left.compute(f) || right.compute(f),
            NormalizedExpr::Eq { column, value } => f(column, value, false),
            NormalizedExpr::NotEq { column, value } => f(column, value, true),
            NormalizedExpr::True => true,
            NormalizedExpr::False => false,
        }
    }
}

fn normalize_predicate_expression(expr: &Expr) -> NormalizedExpr {
    // Returned for unsupported expressions, which are converted to TRUE.
    let unhandled = NormalizedExpr::True;

    match expr {
        Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr { left, op, right }) => match op {
            Operator::And => {
                let left = normalize_predicate_expression(left);
                let right = normalize_predicate_expression(right);
                NormalizedExpr::And {
                    left: left.boxed(),
                    right: right.boxed(),
                }
            }
            Operator::Or => {
                let left = normalize_predicate_expression(left);
                let right = normalize_predicate_expression(right);
                NormalizedExpr::Or {
                    left: left.boxed(),
                    right: right.boxed(),
                }
            }
            Operator::Eq => normalize_equal_expr(left, right, true),
            Operator::NotEq => normalize_equal_expr(left, right, false),
            _ => unhandled,
        },
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) if list.len() < MAX_ELEMS_IN_LIST_FOR_FILTER => {
            if list.is_empty() {
                if *negated {
                    // "not in empty list" is always true
                    NormalizedExpr::True
                } else {
                    // "in empty list" is always false
                    NormalizedExpr::False
                }
            } else {
                let eq_fun = if *negated { Expr::not_eq } else { Expr::eq };
                let re_fun = if *negated { Expr::and } else { Expr::or };
                let transformed_expr = list
                    .iter()
                    .map(|e| eq_fun(*expr.clone(), e.clone()))
                    .reduce(re_fun)
                    .unwrap();
                normalize_predicate_expression(&transformed_expr)
            }
        }
        _ => unhandled,
    }
}

/// Normalize the equal expr as: `column = value` or `column != value`.
///
/// Return [`NormalizedExpr::True`] if it can't be normalized.
fn normalize_equal_expr(left: &Expr, right: &Expr, is_equal: bool) -> NormalizedExpr {
    let (column, value) = match (left, right) {
        (Expr::Column(col), Expr::Literal(val)) => (col, val),
        (Expr::Literal(val), Expr::Column(col)) => (col, val),
        _ => return NormalizedExpr::True,
    };
    let (column, value) = (column.clone(), value.clone());
    if is_equal {
        NormalizedExpr::Eq { column, value }
    } else {
        NormalizedExpr::NotEq { column, value }
    }
}