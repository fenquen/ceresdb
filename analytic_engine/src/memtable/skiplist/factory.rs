// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Skiplist memtable factory

use std::sync::{atomic::AtomicU64, Arc};

use arena::MonoIncArena;
use skiplist::SkipList;

use crate::memtable::{
    factory::{Factory, Options, Result},
    skiplist::{BytewiseComparator, SkipListMemTable},
    MemTableRef,
};

#[derive(Debug)]
pub struct SkiplistMemTableFactory;

impl Factory for SkiplistMemTableFactory {
    fn createMemTable(&self, opts: Options) -> Result<MemTableRef> {
        let monoIncArena = MonoIncArena::with_collector(opts.arena_block_size as usize, opts.collector);
        let skipList = SkipList::with_arena(BytewiseComparator, monoIncArena);
        let memTable = Arc::new(SkipListMemTable {
            schema: opts.schema,
            skiplist: skipList,
            last_sequence: AtomicU64::new(opts.creation_sequence),
            metrics: Default::default(),
        });

        Ok(memTable)
    }
}
