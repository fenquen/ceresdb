// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Row group pruner.

use std::cmp::Ordering;

use arrow::datatypes::SchemaRef;
use common_types::datum::Datum;
use datafusion::{prelude::Expr, scalar::ScalarValue};
use log::debug;
use parquet::file::metadata::RowGroupMetaData;
use parquet_ext::prune::{
    equal::{self, ColumnPosition},
    min_max,
};
use snafu::ensure;
use trace_metric::{MetricsCollector, TraceMetricWhenDrop};

use crate::sst::{
    parquet::meta_data::ParquetFilter,
    reader::error::{OtherNoCause, Result},
};

#[derive(Default, Debug, Clone, TraceMetricWhenDrop)]
struct Metrics {
    #[metric(boolean)]
    use_custom_filter: bool,
    #[metric(number)]
    total_row_groups: usize,
    #[metric(number)]
    row_groups_after_prune: usize,
    #[metric(number)]
    pruned_by_custom_filter: usize,
    #[metric(number)]
    pruned_by_min_max: usize,
    #[metric(collector)]
    collector: Option<MetricsCollector>,
}

/// RowGroupPruner is used to prune row groups according to the provided
/// predicates and filters.
///
/// Currently, two kinds of filters will be applied to such filtering:
/// min max & parquet_filter.
pub struct RowGroupPruner<'a> {
    schema: &'a SchemaRef,
    row_groups: &'a [RowGroupMetaData],
    parquet_filter: Option<&'a ParquetFilter>,
    predicates: &'a [Expr],
    metrics: Metrics,
}

impl<'a> RowGroupPruner<'a> {
    // TODO: DataFusion already change predicates to PhyscialExpr, we should keep up with upstream.
    // https://github.com/apache/arrow-datafusion/issues/4695
    pub fn try_new(schema: &'a SchemaRef,
                   row_groups: &'a [RowGroupMetaData],
                   parquet_filter: Option<&'a ParquetFilter>,
                   predicates: &'a [Expr],
                   metrics_collector: Option<MetricsCollector>) -> Result<Self> {
        if let Some(f) = parquet_filter {
            ensure!(f.len() == row_groups.len(), OtherNoCause {msg: format!("expect sst_filters.len() == row_groups.len(), num_sst_filters:{}, num_row_groups:{}", f.len(), row_groups.len()),});
        }

        let metrics = Metrics {
            use_custom_filter: parquet_filter.is_some(),
            total_row_groups: row_groups.len(),
            collector: metrics_collector,
            ..Default::default()
        };

        Ok(Self {
            schema,
            row_groups,
            parquet_filter,
            predicates,
            metrics,
        })
    }

    pub fn prune(&mut self) -> Vec<usize> {
        debug!(
            "Begin to prune row groups, total_row_groups:{}, parquet_filter:{}, predicates:{:?}",
            self.row_groups.len(),
            self.parquet_filter.is_some(),
            self.predicates,
        );

        let pruned0 = self.prune_by_min_max();
        self.metrics.pruned_by_min_max = self.row_groups.len() - pruned0.len();

        let pruned = match self.parquet_filter {
            Some(v) => {
                // TODO: We can do continuous prune based on the `pruned0` to reduce the
                // filtering cost.
                let pruned1 = self.prune_by_filters(v);
                let pruned = Self::intersect_pruned_row_groups(&pruned0, &pruned1);

                self.metrics.pruned_by_custom_filter = self.row_groups.len() - pruned1.len();
                debug!(
                    "Finish pruning row groups by parquet_filter and min_max, total_row_groups:{}, pruned_by_min_max:{}, pruned_by_blooms:{}, pruned_by_both:{}",
                    self.row_groups.len(),
                    pruned0.len(),
                    pruned1.len(),
                    pruned.len(),
                );

                pruned
            }
            None => {
                debug!(
                    "Finish pruning row groups by min_max, total_row_groups:{}, pruned_row_groups:{}",
                    self.row_groups.len(),
                    pruned0.len(),
                );
                pruned0
            }
        };

        self.metrics.row_groups_after_prune = pruned.len();
        pruned
    }

    fn prune_by_min_max(&self) -> Vec<usize> {
        min_max::prune_row_groups(self.schema.clone(), self.predicates, self.row_groups)
    }

    /// Prune row groups according to the filter.
    fn prune_by_filters(&self, parquet_filter: &ParquetFilter) -> Vec<usize> {
        let is_equal =
            |col_pos: ColumnPosition, val: &ScalarValue, negated: bool| -> Option<bool> {
                let datum = Datum::from_scalar_value(val)?;
                let exist = parquet_filter[col_pos.row_group_idx]
                    .contains_column_data(col_pos.column_idx, &datum.to_bytes())?;
                if exist {
                    // parquet_filter has false positivity, that is to say we are unsure whether
                    // this value exists even if the parquet_filter says it
                    // exists.
                    None
                } else {
                    Some(negated)
                }
            };

        equal::prune_row_groups(
            self.schema.clone(),
            self.predicates,
            self.row_groups.len(),
            is_equal,
        )
    }

    /// Compute the intersection of the two row groups which are in increasing
    /// order.
    fn intersect_pruned_row_groups(row_groups0: &[usize], row_groups1: &[usize]) -> Vec<usize> {
        let mut intersect = Vec::with_capacity(row_groups0.len().min(row_groups1.len()));

        let (mut i0, mut i1) = (0, 0);
        while i0 < row_groups0.len() && i1 < row_groups1.len() {
            let idx0 = row_groups0[i0];
            let idx1 = row_groups1[i1];

            match idx0.cmp(&idx1) {
                Ordering::Less => i0 += 1,
                Ordering::Greater => i1 += 1,
                Ordering::Equal => {
                    intersect.push(idx0);
                    i0 += 1;
                    i1 += 1;
                }
            }
        }

        intersect
    }
}