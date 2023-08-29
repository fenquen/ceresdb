// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Skiplist memtable iterator

use std::{cmp::Ordering, iter::Rev, ops::Bound, time::Instant};

use arena::{Arena, BasicStats};
use bytes_ext::{Bytes, BytesMut};
use codec::row;
use common_types::{
    projected_schema::{ProjectedSchema, RowProjector},
    record_batch::{RecordBatchWithKey, RecordBatchWithKeyBuilder},
    row::contiguous::{ContiguousRowReader, ProjectedContiguousRow},
    schema::Schema,
    SequenceNumber,
};
use generic_error::BoxError;
use log::trace;
use skiplist::{ArenaSlice, SkipListIter, SkipList};
use snafu::ResultExt;
use time_ext::InstantExt;

use crate::memtable::{
    key::{self, KeySequence},
    skiplist::{BytewiseComparator, SkipListMemTable},
    AppendRow, BuildRecordBatch, DecodeContinuousRow, DecodeInternalKey, EncodeInternalKey,
    IterReverse, IterTimeout, ProjectSchema, Result, ScanContext, ScanRequest,
};

#[derive(Debug, PartialEq)]
enum State {
    /// The iterator struct is created but not initialized
    Uninitialized,
    /// The iterator is initialized (seek)
    Initialized,
    /// No more element the iterator can return
    Finished,
}

/// 用来迭代跳表的
pub struct ColumnarIterImpl<A: Arena<Stats=BasicStats> + Clone + Sync + Send> {
    /// the internal skiplist iter
    skipListIter: SkipListIter<SkipList<BytewiseComparator, A>, BytewiseComparator, A>,

    // Schema related:
    /// schema of this memtable to decode row
    memtable_schema: Schema,
    /// Projection of schema to read
    projected_schema: ProjectedSchema,
    projector: RowProjector,

    // Options related:
    batchSize: usize,
    deadline: Option<Instant>,

    /// 目前的话start和end其实都未用到值都是unbound fenquen
    start_user_key: Bound<Bytes>,
    /// 目前的话start和end其实都未用到值都是unbound fenquen
    end_user_key: Bound<Bytes>,

    /// max visible sequence
    maxVisibleSeq: SequenceNumber,

    /// state of iterator
    state: State,

    /// Last internal key this iterator returned
    // TODO(yingwen): Wrap a internal key struct?
    last_internal_key: Option<ArenaSlice<A>>,

    /// dedup rows with key
    need_dedup: bool,
}

impl<A: Arena<Stats=BasicStats> + Clone + Sync + Send> Iterator for ColumnarIterImpl<A> {
    type Item = Result<RecordBatchWithKey>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.state != State::Initialized {
            return None;
        }

        self.fetchNextRecordBatch().transpose()
    }
}

impl<A: Arena<Stats=BasicStats> + Clone + Sync + Send> ColumnarIterImpl<A> {
    pub fn new(memtable: &SkipListMemTable<A>, ctx: ScanContext, request: ScanRequest) -> Result<Self> {
        // create projection for the memtable schema
        let projector = request.projected_schema.try_project_with_key(&memtable.schema).context(ProjectSchema)?;

        let skipListIter = memtable.skiplist.iter();

        let mut columnarIter =
            Self {
                skipListIter,
                memtable_schema: memtable.schema.clone(),
                projected_schema: request.projected_schema,
                projector,
                batchSize: ctx.batch_size,
                deadline: ctx.deadline,
                start_user_key: request.start_user_key,
                end_user_key: request.end_user_key,
                maxVisibleSeq: request.maxVisibleSeq,
                state: State::Uninitialized,
                last_internal_key: None,
                need_dedup: request.need_dedup,
            };

        columnarIter.init()?;

        Ok(columnarIter)
    }

    /// Init the iterator, will seek to the proper position for first next()
    /// call, so the first entry next() returned is after the
    /// `start_user_key`, but we still need to check `end_user_key`
    fn init(&mut self) -> Result<()> {
        match &self.start_user_key {
            Bound::Included(user_key) => {
                // construct seek key
                let mut key_buf = BytesMut::new();
                let seekKey = key::buildSeekKey(user_key, self.maxVisibleSeq, &mut key_buf).context(EncodeInternalKey)?;

                self.skipListIter.seek(seekKey);
            }
            Bound::Excluded(user_key) => {
                // construct seek key, just seek to the key with next prefix, so there is no
                // need to skip the key until we meet the first key > start_user_key
                let next_user_key = row::key_prefix_next(user_key);

                // construct seek key
                let mut key_buf = BytesMut::new();
                let seek_key = key::buildSeekKey(&next_user_key, self.maxVisibleSeq, &mut key_buf).context(EncodeInternalKey)?;

                self.skipListIter.seek(seek_key);
            }
            Bound::Unbounded => self.skipListIter.seekToFirst(),
        }

        self.state = State::Initialized;

        Ok(())
    }

    fn fetchNextRecordBatch(&mut self) -> Result<Option<RecordBatchWithKey>> {
        debug_assert_eq!(State::Initialized, self.state);
        assert!(self.batchSize > 0);

        let mut builder =
            RecordBatchWithKeyBuilder::with_capacity(self.projected_schema.to_record_schema_with_key(),
                                                     self.batchSize);
        let mut num_rows = 0;

        while self.skipListIter.valid() && num_rows < self.batchSize {
            if let Some(rowData) = self.fetchNextRow()? {
                let rowReader = ContiguousRowReader::new(&rowData, &self.memtable_schema).context(DecodeContinuousRow)?;
                let projectedRow = ProjectedContiguousRow::new(rowReader, &self.projector);

                trace!("column iterator fetch next row, row:{:?}", projectedRow);

                builder.appendProjectedContiguousRow(&projectedRow).context(AppendRow)?;
                num_rows += 1;
            } else { // there is no more row to fetch
                self.finish();
                break;
            }
        }

        if num_rows > 0 {
            if let Some(deadline) = self.deadline {
                if deadline.check_deadline() {
                    return IterTimeout {}.fail();
                }
            }

            let batch = builder.build().context(BuildRecordBatch)?;
            trace!("column iterator send one batch:{:?}", batch);

            Ok(Some(batch))
        } else {
            // if iter is invalid after seek (nothing matched), then it may not be marked as finished yet
            self.finish();
            Ok(None)
        }
    }

    /// fetch next row matched the given condition, the current entry of iter will be considered
    fn fetchNextRow(&mut self) -> Result<Option<ArenaSlice<A>>> {
        debug_assert_eq!(State::Initialized, self.state);

        // TODO(yingwen): Some operation like delete needs to be considered during iterating: we need to ignore this key if found a delete mark
        while self.skipListIter.valid() {
            let key = self.skipListIter.key();

            // 实际上保存到内部的跳表中的key是internalKey是在原始的key的底子上再加上8个字节sequence和4个字节rowindex的 fenquen
            let (userKey, keySequence) = key::extractUserKeyFromInternalKey(key).context(DecodeInternalKey)?;

            // check user key is  out of bound
            if self.isAfterEndBound(userKey) {
                self.finish();
                return Ok(None);
            }

            if self.need_dedup {
                // whether this user key is already returned
                let same_key = match &self.last_internal_key {
                    Some(last_internal_key) => {
                        // TODO(yingwen): Actually this call wont fail, only valid internal key will be set as last_internal_key so maybe we can just unwrap it?
                        let (last_user_key, _) = key::extractUserKeyFromInternalKey(last_internal_key).context(DecodeInternalKey)?;
                        userKey == last_user_key
                    }
                    None => false,// this is the first user key
                };

                // meet a duplicate key, move forward and continue to find next user key
                if same_key {
                    self.skipListIter.next();
                    continue;
                }
            }

            // Check whether this key is visible
            if !self.isVisible(keySequence) {
                // The sequence of this key is not visible, move forward
                self.skipListIter.next();
                continue;
            }

            let row = self.skipListIter.value_with_arena();

            // store the last key
            self.last_internal_key = Some(self.skipListIter.key_with_arena());

            // forward
            self.skipListIter.next();

            return Ok(Some(row));
        }

        // no more row in range, we can stop the iterator
        self.finish();

        Ok(None)
    }

    /// Return true if the sequence is visible
    #[inline]
    fn isVisible(&self, sequence: KeySequence) -> bool {
        sequence.sequence() <= self.maxVisibleSeq
    }

    /// return true if the key is after the `end_user_key` bound
    fn isAfterEndBound(&self, key: &[u8]) -> bool {
        match &self.end_user_key {
            Bound::Included(end) => match key.cmp(end) {
                Ordering::Less | Ordering::Equal => false,
                Ordering::Greater => true,
            },
            Bound::Excluded(end) => match key.cmp(end) {
                Ordering::Less => false,
                Ordering::Equal | Ordering::Greater => true,
            },
            // all key is valid
            Bound::Unbounded => false,
        }
    }

    /// Mark the iterator state to finished and return None
    fn finish(&mut self) {
        self.state = State::Finished;
    }
}

/// Reversed columnar iterator.
// TODO(xikai): Now the implementation is not perfect: read all the entries
//  into a buffer and reverse read it. The memtable should support scan in
// reverse  order naturally.
pub struct ReversedColumnarIterator<I> {
    iter: I,
    reversed_iter: Option<Rev<std::vec::IntoIter<Result<RecordBatchWithKey>>>>,
    num_record_batch: usize,
}

impl<I> ReversedColumnarIterator<I>
    where
        I: Iterator<Item=Result<RecordBatchWithKey>>,
{
    pub fn new(iter: I, num_rows: usize, batch_size: usize) -> Self {
        Self {
            iter,
            reversed_iter: None,
            num_record_batch: num_rows / batch_size,
        }
    }

    fn init_if_necessary(&mut self) {
        if self.reversed_iter.is_some() {
            return;
        }

        let mut buf = Vec::with_capacity(self.num_record_batch);
        for item in &mut self.iter {
            buf.push(item);
        }
        self.reversed_iter = Some(buf.into_iter().rev());
    }
}

impl<I> Iterator for ReversedColumnarIterator<I> where I: Iterator<Item=Result<RecordBatchWithKey>> {
    type Item = Result<RecordBatchWithKey>;

    fn next(&mut self) -> Option<Self::Item> {
        self.init_if_necessary();
        self.reversed_iter
            .as_mut()
            .unwrap()
            .next()
            .map(|v| match v {
                Ok(mut batch_with_key) => {
                    batch_with_key
                        .reverse_data()
                        .box_err()
                        .context(IterReverse)?;

                    Ok(batch_with_key)
                }
                Err(e) => Err(e),
            })
    }
}

// TODO(yingwen): Test
