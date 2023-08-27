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
use skiplist::{KeyComparator, SkipList};
use snafu::{ensure, ResultExt};

use crate::memtable::{
    key::{ComparableInternalKey, KeySequence},
    skiplist::iter::{ColumnarIterImpl, ReversedColumnarIterator},
    ColumnarIter, EncodeInternalKey, InvalidPutSequence, InvalidRow, MemTable,
    Metrics as MemtableMetrics, PutContext, Result, ScanContext, ScanRequest,
};

#[derive(Default, Debug)]
struct Metrics {
    row_raw_size: AtomicUsize,
    row_encoded_size: AtomicUsize,
    row_count: AtomicUsize,
}

pub struct SkipListMemTable<A: Arena<Stats=BasicStats> + Clone + Sync + Send> {
    /// Schema of this memtable, is immutable.
    schema: Schema,
    skiplist: SkipList<BytewiseComparator, A>,
    /// The last sequence of the rows in this memtable. Update to this field require external synchronization.
    last_sequence: AtomicU64,

    metrics: Metrics,
}

impl<A: Arena<Stats=BasicStats> + Clone + Sync + Send + 'static> MemTable for SkipListMemTable<A> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn min_key(&self) -> Option<Bytes> {
        let mut iter = self.skiplist.iter();
        iter.seekToFirst();
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
    // Now the caller is required to encode the row into the `value_buf` in PutContext first.
    fn put(&self,
           putContext: &mut PutContext,
           keySequence: KeySequence,
           row: &Row,
           schema: &Schema) -> Result<()> {
        trace!("skiplist put row, keySequence:{:?}, row:{:?}", keySequence, row);

        let comparableInternalKey = ComparableInternalKey::new(keySequence, schema);

        //let keyByteVec = &mut ctx.key_buf;
        // Reset key buffer
        putContext.keyBuf.clear();
        // Reserve capacity for key
        putContext.keyBuf.reserve(comparableInternalKey.estimateEncodedSize(row));
        // Encode key primary的列值+剩下的sequence量+剩下的rowId量
        comparableInternalKey.encode(&mut putContext.keyBuf, row).context(EncodeInternalKey)?;

        // encode row value. The ContiguousRowWriter will clear the buf.
       // let row_value = &mut putContext.valueBuf;
        let mut rowWriter = ContiguousRowWriter::new(&mut putContext.valueBuf, schema, &putContext.index_in_writer);
        rowWriter.writeRow(row).box_err().context(InvalidRow)?;

        self.skiplist.put(&putContext.keyBuf, &putContext.valueBuf);

        // update metrics
        self.metrics.row_raw_size.fetch_add(row.size(), atomic::Ordering::Relaxed);
        self.metrics.row_count.fetch_add(1, atomic::Ordering::Relaxed);
        self.metrics.row_encoded_size.fetch_add(putContext.keyBuf.len() + putContext.valueBuf.len(), atomic::Ordering::Relaxed);

        Ok(())
    }

    fn scan(&self, ctx: ScanContext, scanRequest: ScanRequest) -> Result<ColumnarIter> {
        debug!("scan skiplist memtable, ctx:{:?}, request:{:?}",ctx, scanRequest);

      //  let num_rows = self.skiplist.len();
        let (reverse, batch_size) = (scanRequest.reverse, ctx.batch_size);
        let iter = ColumnarIterImpl::new(self, ctx, scanRequest)?;
        if reverse {
            Ok(Box::new(ReversedColumnarIterator::new(iter, self.skiplist.len(), batch_size)))
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
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        lhs.cmp(rhs)
    }

    #[inline]
    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        lhs == rhs
    }
}