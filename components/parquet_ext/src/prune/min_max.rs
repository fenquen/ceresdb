// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use arrow::{array::ArrayRef, datatypes::Schema as ArrowSchema};
use datafusion::{
    common::ToDFSchema,
    error::Result as DataFusionResult,
    physical_expr::{create_physical_expr, execution_props::ExecutionProps},
    physical_optimizer::pruning::{PruningPredicate, PruningStatistics},
    physical_plan::PhysicalExpr,
    prelude::{Column, Expr},
    scalar::ScalarValue,
};
use log::{error, trace};
use parquet::file::{metadata::RowGroupMetaData, statistics::Statistics as ParquetStatistics};

/// Filters row groups according to the predicate function, and returns the
/// indexes of the filtered row groups.
pub fn prune_row_groups(
    schema: Arc<ArrowSchema>,
    exprs: &[Expr],
    row_groups: &[RowGroupMetaData],
) -> Vec<usize> {
    let mut target_row_groups = Vec::with_capacity(row_groups.len());
    let should_reads = filter_row_groups_inner(schema, exprs, row_groups);
    for (i, should_read) in should_reads.iter().enumerate() {
        if *should_read {
            target_row_groups.push(i);
        }
    }

    target_row_groups
}

/// Determine whether a row group should be read according to the meta data
/// in the `row_groups`.
///
/// The boolean value in the returned vector denotes the corresponding row
/// group in the `row_groups` whether should be read.
fn filter_row_groups_inner(
    schema: Arc<ArrowSchema>,
    exprs: &[Expr],
    row_groups: &[RowGroupMetaData],
) -> Vec<bool> {
    let mut results = vec![true; row_groups.len()];
    for expr in exprs {
        match logical2physical(expr, &schema)
            .and_then(|physical_expr| PruningPredicate::try_new(physical_expr, schema.clone()))
        {
            Ok(pruning_predicate) => {
                trace!("pruning_predicate is:{:?}", pruning_predicate);

                if let Ok(values) = build_row_group_predicate(&pruning_predicate, row_groups) {
                    for (curr_val, result_val) in values.into_iter().zip(results.iter_mut()) {
                        *result_val = curr_val && *result_val
                    }
                };
                // if fail to build, just ignore this filter so that all the
                // row groups should be read for this
                // filter.
            }
            Err(e) => {
                // for any error just ignore it and that is to say, for this filter all the row
                // groups should be read.
                error!("fail to build pruning predicate, err:{}", e);
            }
        }
    }

    results
}

fn logical2physical(expr: &Expr, schema: &ArrowSchema) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
    schema.clone().to_dfschema().and_then(|df_schema| {
        // TODO: props should be an argument
        let execution_props = ExecutionProps::new();
        create_physical_expr(expr, &df_schema, schema, &execution_props)
    })
}

fn build_row_group_predicate(
    predicate_builder: &PruningPredicate,
    row_group_metadata: &[RowGroupMetaData],
) -> datafusion::common::Result<Vec<bool>> {
    let parquet_schema = predicate_builder.schema().as_ref();

    let pruning_stats = RowGroupPruningStatistics {
        row_group_metadata,
        parquet_schema,
    };

    predicate_builder.prune(&pruning_stats)
}

/// port from datafusion.
/// Extract the min/max statistics from a `ParquetStatistics` object
macro_rules! get_statistic {
    ($column_statistics:expr, $func:ident, $bytes_func:ident) => {{
        if !$column_statistics.has_min_max_set() {
            return None;
        }
        match $column_statistics {
            ParquetStatistics::Boolean(s) => Some(ScalarValue::Boolean(Some(*s.$func()))),
            ParquetStatistics::Int32(s) => Some(ScalarValue::Int32(Some(*s.$func()))),
            ParquetStatistics::Int64(s) => Some(ScalarValue::Int64(Some(*s.$func()))),
            // 96 bit ints not supported
            ParquetStatistics::Int96(_) => None,
            ParquetStatistics::Float(s) => Some(ScalarValue::Float32(Some(*s.$func()))),
            ParquetStatistics::Double(s) => Some(ScalarValue::Float64(Some(*s.$func()))),
            ParquetStatistics::ByteArray(s) => {
                let s = std::str::from_utf8(s.$bytes_func())
                    .map(|s| s.to_string())
                    .ok();
                Some(ScalarValue::Utf8(s))
            }
            // type not supported yet
            ParquetStatistics::FixedLenByteArray(_) => None,
        }
    }};
}

/// port from datafusion.
// Extract the min or max value calling `func` or `bytes_func` on the
// ParquetStatistics as appropriate
macro_rules! get_min_max_values {
    ($self:expr, $column:expr, $func:ident, $bytes_func:ident) => {{
        let (column_index, field) =
            if let Some((v, f)) = $self.parquet_schema.column_with_name(&$column.name) {
                (v, f)
            } else {
                // Named column was not present
                return None;
            };

        let data_type = field.data_type();
        let null_scalar: ScalarValue = if let Ok(v) = data_type.try_into() {
            v
        } else {
            // DataFusion doesn't have support for ScalarValues of the column type
            return None;
        };

        let scalar_values: Vec<ScalarValue> = $self
            .row_group_metadata
            .iter()
            .flat_map(|meta| meta.column(column_index).statistics())
            .map(|stats| get_statistic!(stats, $func, $bytes_func))
            .map(|maybe_scalar| {
                // column either did't have statistics at all or didn't have min/max values
                maybe_scalar.unwrap_or_else(|| null_scalar.clone())
            })
            .collect();

        // ignore errors converting to arrays (e.g. different types)
        ScalarValue::iter_to_array(scalar_values).ok()
    }};
}

/// Wraps parquet statistics in a way
/// that implements [`PruningStatistics`]
struct RowGroupPruningStatistics<'a> {
    row_group_metadata: &'a [RowGroupMetaData],
    parquet_schema: &'a ArrowSchema,
}

impl<'a> PruningStatistics for RowGroupPruningStatistics<'a> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        get_min_max_values!(self, column, min, min_bytes)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        get_min_max_values!(self, column, max, max_bytes)
    }

    fn num_containers(&self) -> usize {
        self.row_group_metadata.len()
    }

    // TODO: support this.
    fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }
}