// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! MemTable based on skiplist

pub mod factory;
pub mod iter;

use std::{
    cmp::Ordering,
    convert::TryInto,
    sync::atomic::{self, AtomicU64, AtomicUsize},
};

use arena::{Arena, BasicStats};
use bytes_ext::Bytes;
use codec::Encoder;
use common_types::{
    row::{contiguous::ContiguousRowWriter, Row},
    schema::Schema,
    SequenceNumber,
};
use generic_error::BoxError;
use log::{debug, trace};
use skiplist::{KeyComparator, Skiplist};
use snafu::{ensure, ResultExt};

use crate::memtable::{
    key::{ComparableInternalKey, KeySequence},
    skiplist::iter::{ColumnarIterImpl, ReversedColumnarIterator},
    ColumnarIterPtr, EncodeInternalKey, InvalidPutSequence, InvalidRow, MemTable,
    Metrics as MemtableMetrics, PutContext, Result, ScanContext, ScanRequest,
};

#[derive(Default, Debug)]
struct Metrics {
    row_raw_size: AtomicUsize,
    row_encoded_size: AtomicUsize,
    row_count: AtomicUsize,
}

/// MemTable implementation based on skiplist
pub struct SkiplistMemTable<A: Arena<Stats = BasicStats> + Clone + Sync + Send> {
    /// Schema of this memtable, is immutable.
    schema: Schema,
    skiplist: Skiplist<BytewiseComparator, A>,
    /// The last sequence of the rows in this memtable. Update to this field
    /// require external synchronization.
    last_sequence: AtomicU64,

    metrics: Metrics,
}

impl<A: Arena<Stats = BasicStats> + Clone + Sync + Send + 'static> MemTable
    for SkiplistMemTable<A>
{
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn min_key(&self) -> Option<Bytes> {
        let mut iter = self.skiplist.iter();
        iter.seek_to_first();
        if !iter.valid() {
            None
        } else {
            Some(iter.key().to_vec().into())
        }
    }

    fn max_key(&self) -> Option<Bytes> {
        let mut iter = self.skiplist.iter();
        iter.seek_to_last();
        if !iter.valid() {
            None
        } else {
            Some(iter.key().to_vec().into())
        }
    }

    // TODO(yingwen): Encode value if value_buf is not set.
    // Now the caller is required to encode the row into the `value_buf` in
    // PutContext first.
    fn put(
        &self,
        ctx: &mut PutContext,
        sequence: KeySequence,
        row: &Row,
        schema: &Schema,
    ) -> Result<()> {
        trace!("skiplist put row, sequence:{:?}, row:{:?}", sequence, row);

        let key_encoder = ComparableInternalKey::new(sequence, schema);

        let internal_key = &mut ctx.key_buf;
        // Reset key buffer
        internal_key.clear();
        // Reserve capacity for key
        internal_key.reserve(key_encoder.estimate_encoded_size(row));
        // Encode key
        key_encoder
            .encode(internal_key, row)
            .context(EncodeInternalKey)?;

        // Encode row value. The ContiguousRowWriter will clear the buf.
        let row_value = &mut ctx.value_buf;
        let mut row_writer = ContiguousRowWriter::new(row_value, schema, &ctx.index_in_writer);
        row_writer.write_row(row).box_err().context(InvalidRow)?;
        let encoded_size = internal_key.len() + row_value.len();
        self.skiplist.put(internal_key, row_value);

        // Update metrics
        self.metrics
            .row_raw_size
            .fetch_add(row.size(), atomic::Ordering::Relaxed);
        self.metrics
            .row_count
            .fetch_add(1, atomic::Ordering::Relaxed);
        self.metrics
            .row_encoded_size
            .fetch_add(encoded_size, atomic::Ordering::Relaxed);

        Ok(())
    }

    fn scan(&self, ctx: ScanContext, request: ScanRequest) -> Result<ColumnarIterPtr> {
        debug!(
            "Scan skiplist memtable, ctx:{:?}, request:{:?}",
            ctx, request
        );

        let num_rows = self.skiplist.len();
        let (reverse, batch_size) = (request.reverse, ctx.batch_size);
        let iter = ColumnarIterImpl::new(self, ctx, request)?;
        if reverse {
            Ok(Box::new(ReversedColumnarIterator::new(
                iter, num_rows, batch_size,
            )))
        } else {
            Ok(Box::new(iter))
        }
    }

    fn approximate_memory_usage(&self) -> usize {
        // Mem size of skiplist is u32, need to cast to usize
        match self.skiplist.mem_size().try_into() {
            Ok(v) => v,
            // The skiplist already use bytes larger than usize
            Err(_) => usize::MAX,
        }
    }

    fn set_last_sequence(&self, sequence: SequenceNumber) -> Result<()> {
        let last = self.last_sequence();
        ensure!(
            sequence >= last,
            InvalidPutSequence {
                given: sequence,
                last
            }
        );

        self.last_sequence
            .store(sequence, atomic::Ordering::Relaxed);

        Ok(())
    }

    fn last_sequence(&self) -> SequenceNumber {
        self.last_sequence.load(atomic::Ordering::Relaxed)
    }

    fn metrics(&self) -> MemtableMetrics {
        let row_raw_size = self.metrics.row_raw_size.load(atomic::Ordering::Relaxed);
        let row_encoded_size = self
            .metrics
            .row_encoded_size
            .load(atomic::Ordering::Relaxed);
        let row_count = self.metrics.row_count.load(atomic::Ordering::Relaxed);
        MemtableMetrics {
            row_raw_size,
            row_encoded_size,
            row_count,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BytewiseComparator;

impl KeyComparator for BytewiseComparator {
    #[inline]
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        lhs.cmp(rhs)
    }

    #[inline]
    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        lhs == rhs
    }
}