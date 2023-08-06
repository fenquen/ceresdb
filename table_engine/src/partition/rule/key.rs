// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Key partition rule

use std::collections::{HashMap, HashSet};

use bytes_ext::{BufMut, BytesMut};
use common_types::{
    datum::Datum,
    row::{Row, RowGroup},
};
use hash_ext::hash64;
use itertools::Itertools;
use log::{debug, error};
use snafu::OptionExt;

use crate::partition::{
    rule::{filter::PartitionCondition, ColumnWithType, PartitionFilter, PartitionRule},
    Internal, LocateWritePartition, Result,
};

pub const DEFAULT_PARTITION_VERSION: i32 = 0;

pub struct KeyRule {
    pub typed_key_columns: Vec<ColumnWithType>,
    pub partition_num: usize,
}

impl KeyRule {
    /// Check and do cartesian product to get candidate partition groups.
    ///
    /// for example:
    ///      key_col1: [f1, f2, f3]
    ///      key_col2: [f4, f5]
    /// will convert to:
    ///      group1: [key_col1: f1, key_col2: f4]
    ///      group2: [key_col1: f1, key_col2: f5]
    ///      group3: [key_col1: f2, key_col2: f4]
    ///      ...
    ///
    /// Above logics are preparing for implementing something like:
    ///     fa1 && fa2 && fb =  (fa1 && fb) && (fa2 && fb)
    /// Partitions about above expression will be calculated by following steps:
    ///     + partitions about "(fa1 && fb)" will be calculated first,
    ///        assume "partitions 1"
    ///     + partitions about "(fa2 && fb)"  will be calculated after,
    ///        assume "partitions 2"
    ///     + "total partitions" = "partitions 1" intersection "partitions 2"
    fn get_candidate_partition_keys_groups(
        &self,
        filters: &[PartitionFilter],
    ) -> Result<Vec<Vec<usize>>> {
        let column_name_to_idxs = self
            .typed_key_columns
            .iter()
            .enumerate()
            .map(|(col_idx, typed_col)| (typed_col.column.clone(), col_idx))
            .collect::<HashMap<_, _>>();
        let mut filter_by_columns = vec![Vec::new(); self.typed_key_columns.len()];

        // Group the filters by their columns.
        for (filter_idx, filter) in filters.iter().enumerate() {
            let col_idx = column_name_to_idxs
                .get(filter.column.as_str())
                .context(Internal {
                    msg: format!(
                        "column in filters but not in target, column:{}, targets:{:?}",
                        filter.column, self.typed_key_columns
                    ),
                })?;

            filter_by_columns
                .get_mut(*col_idx)
                .unwrap()
                .push(filter_idx);
        }
        debug!(
            "KeyRule get candidate partition keys groups, filter_by_columns:{:?}",
            filter_by_columns
        );

        let empty_filter = filter_by_columns.iter().find(|filter| filter.is_empty());
        if empty_filter.is_some() {
            return Ok(Vec::default());
        }

        let groups = filter_by_columns
            .into_iter()
            .map(|filters| filters.into_iter())
            .multi_cartesian_product()
            .collect_vec();
        debug!(
            "KeyRule get candidate partition keys groups, groups:{:?}",
            groups
        );

        Ok(groups)
    }

    fn compute_partition_for_inserted_row(
        &self,
        row: &Row,
        target_column_idxs: &[usize],
        buf: &mut BytesMut,
    ) -> usize {
        let partition_keys = target_column_idxs
            .iter()
            .map(|col_idx| &row[*col_idx])
            .collect_vec();
        compute_partition(&partition_keys, self.partition_num, buf)
    }

    fn compute_partition_for_keys_group(
        &self,
        group: &[usize],
        filters: &[PartitionFilter],
        buf: &mut BytesMut,
    ) -> Result<HashSet<usize>> {
        buf.clear();

        let mut partitions = HashSet::new();
        let expanded_group = expand_partition_keys_group(group, filters)?;
        for partition_keys in expanded_group {
            let partition_key_refs = partition_keys.iter().collect_vec();
            let partition = compute_partition(&partition_key_refs, self.partition_num, buf);
            partitions.insert(partition);
        }

        Ok(partitions)
    }
}

impl PartitionRule for KeyRule {
    fn columns(&self) -> Vec<String> {
        self.typed_key_columns
            .iter()
            .map(|typed_col| typed_col.column.clone())
            .collect()
    }

    fn locate_partitions_for_write(&self, row_group: &RowGroup) -> Result<Vec<usize>> {
        // Extract idxs.
        // TODO: we should compare column's related data types in `typed_key_columns`
        // and the ones in `row_group`'s schema.
        let typed_idxs = self
            .typed_key_columns
            .iter()
            .map(|typed_col| row_group.schema().index_of(typed_col.column.as_str()))
            .collect::<Option<Vec<_>>>()
            .context(LocateWritePartition {
                msg: format!(
                    "not all key columns found in schema when locate partition by key strategy, key columns:{:?}",
                    self.typed_key_columns
                ),
            })?;

        // Compute partitions.
        let mut buf = BytesMut::new();
        let partitions = row_group
            .iter()
            .map(|row| self.compute_partition_for_inserted_row(row, &typed_idxs, &mut buf))
            .collect();
        Ok(partitions)
    }

    fn locate_partitions_for_read(&self, filters: &[PartitionFilter]) -> Result<Vec<usize>> {
        let all_partitions = (0..self.partition_num).collect();

        // Filters are empty.
        if filters.is_empty() {
            return Ok(all_partitions);
        }

        // Group the filters by their columns.
        // If found invalid filter, return all partitions.
        let candidate_partition_keys_groups = self
            .get_candidate_partition_keys_groups(filters)
            .map_err(|e| {
                error!("KeyRule locate partition for read, err:{}", e);
            })
            .unwrap_or_default();
        if candidate_partition_keys_groups.is_empty() {
            return Ok(all_partitions);
        }

        let mut buf = BytesMut::new();
        let (first_group, rest_groups) = candidate_partition_keys_groups.split_first().unwrap();
        let mut target_partitions =
            self.compute_partition_for_keys_group(first_group, filters, &mut buf)?;
        for group in rest_groups {
            // Same as above, if found invalid, return all partitions.
            let partitions = match self.compute_partition_for_keys_group(group, filters, &mut buf) {
                Ok(partitions) => partitions,
                Err(e) => {
                    error!("KeyRule locate partition for read, err:{}", e);
                    return Ok(all_partitions);
                }
            };

            target_partitions = target_partitions
                .intersection(&partitions)
                .copied()
                .collect::<HashSet<_>>();
        }

        Ok(target_partitions.into_iter().collect())
    }
}

fn expand_partition_keys_group(
    group: &[usize],
    filters: &[PartitionFilter],
) -> Result<Vec<Vec<Datum>>> {
    let mut datum_by_columns = Vec::with_capacity(group.len());
    for filter_idx in group {
        let filter = &filters[*filter_idx];
        let datums = match &filter.condition {
            // Only `Eq` is supported now.
            // TODO: to support `In`'s extracting.
            PartitionCondition::Eq(datum) => vec![datum.clone()],
            PartitionCondition::In(datums) => datums.clone(),
            _ => {
                return Internal {
                    msg: format!("invalid partition filter found, filter:{filter:?},"),
                }
                .fail()
            }
        };

        datum_by_columns.push(datums);
    }

    let expanded_group = datum_by_columns
        .into_iter()
        .map(|filters| filters.into_iter())
        .multi_cartesian_product()
        .collect_vec();
    Ok(expanded_group)
}

// Compute partition
pub(crate) fn compute_partition(
    partition_keys: &[&Datum],
    partition_num: usize,
    buf: &mut BytesMut,
) -> usize {
    buf.clear();
    partition_keys
        .iter()
        .for_each(|datum| buf.put_slice(&datum.to_bytes()));

    (hash64(buf) % (partition_num as u64)) as usize
}