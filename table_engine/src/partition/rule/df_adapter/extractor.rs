// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partition filter extractor

use std::collections::HashSet;

use common_types::datum::Datum;
use datafusion::logical_expr::{expr::InList, Expr, Operator};
use df_operator::visitor::find_columns_by_expr;

use crate::partition::rule::filter::{PartitionCondition, PartitionFilter};

/// The datafusion filter exprs extractor
///
/// It's used to extract the meaningful `Expr`s and convert them to
/// [PartitionFilter](the inner filter type in ceresdb).
///
/// NOTICE: When you implements [PartitionRule] for specific partition strategy,
/// you should implement the corresponding [FilterExtractor], too.
///
/// For example: [KeyRule] and [KeyExtractor].
/// If they are not related, [PartitionRule] may not take effect.
pub trait FilterExtractor: Send + Sync + 'static {
    fn extract(&self, filters: &[Expr], columns: &[String]) -> Vec<PartitionFilter>;
}
pub struct KeyExtractor;

impl FilterExtractor for KeyExtractor {
    fn extract(&self, filters: &[Expr], columns: &[String]) -> Vec<PartitionFilter> {
        if filters.is_empty() {
            return Vec::default();
        }

        let mut target = Vec::with_capacity(filters.len());
        for filter in filters {
            // If no target columns included in `filter`, ignore this `filter`.
            let columns_in_filter = find_columns_by_expr(filter)
                .into_iter()
                .collect::<HashSet<_>>();
            let find_result = columns
                .iter()
                .find(|col| columns_in_filter.contains(col.as_str()));

            if find_result.is_none() {
                continue;
            }

            // If target columns included, now only the situation that only target column in
            // filter is supported. Once other type column found here, we ignore it.
            // TODO: support above situation.
            if columns_in_filter.len() != 1 {
                continue;
            }

            // Finally, we try to convert `filter` to `PartitionFilter`.
            // We just support the simple situation: "colum = value" now.
            // TODO: support "colum in [value list]".
            // TODO: we need to compare and check the datatype of column and value.
            // (Actually, there is type conversion on high-level, but when converted data
            // is overflow, it may take no effect).
            let partition_filter = match filter.clone() {
                Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr { left, op, right }) => {
                    match (*left, op, *right) {
                        (Expr::Column(col), Operator::Eq, Expr::Literal(val))
                        | (Expr::Literal(val), Operator::Eq, Expr::Column(col)) => {
                            let datum_opt = Datum::from_scalar_value(&val);
                            datum_opt.map(|datum| {
                                PartitionFilter::new(col.name, PartitionCondition::Eq(datum))
                            })
                        }
                        _ => None,
                    }
                }
                Expr::InList(InList {
                    expr,
                    list,
                    negated,
                }) => {
                    if let (Expr::Column(col), list, false) = (*expr, list, negated) {
                        let mut datums = Vec::with_capacity(list.len());
                        for entry in list {
                            if let Expr::Literal(val) = entry {
                                let datum_opt = Datum::from_scalar_value(&val);
                                if let Some(datum) = datum_opt {
                                    datums.push(datum)
                                }
                            }
                        }
                        if datums.is_empty() {
                            None
                        } else {
                            Some(PartitionFilter::new(
                                col.name,
                                PartitionCondition::In(datums),
                            ))
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            };

            if let Some(pf) = partition_filter {
                target.push(pf);
            }
        }

        target
    }
}

pub type FilterExtractorRef = Box<dyn FilterExtractor>;