// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! utilities for manipulating arrow/parquet/datafusion data structures.

use std::convert::TryFrom;

use arrow::{
    array::UInt32Array,
    compute,
    error::{ArrowError, Result},
    record_batch::RecordBatch,
};

/// Reverse the data in the [`RecordBatch`] by read and copy from the source
/// `batch`.
pub fn reverse_record_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let reversed_columns = {
        let num_rows = u32::try_from(batch.num_rows()).map_err(|e| {
            ArrowError::InvalidArgumentError(format!(
                "too many rows in a batch, convert usize to u32 failed, num_rows:{}, err:{}",
                batch.num_rows(),
                e
            ))
        })?;
        // TODO(xikai): avoid this memory allocation.
        let indices = UInt32Array::from_iter_values((0..num_rows).rev());

        let mut cols = Vec::with_capacity(batch.num_columns());
        for orig_col_data in batch.columns() {
            let new_col_data = compute::take(orig_col_data.as_ref(), &indices, None)?;
            cols.push(new_col_data);
        }

        cols
    };

    RecordBatch::try_new(batch.schema(), reversed_columns)
}