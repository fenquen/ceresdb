// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Forked from <https://github.com/tikv/agatedb/blob/8510bff2bfde5b766c3f83cf81c00141967d48a4/skiplist>
//!
//! Differences:
//! 1. Inline key and value in Node, so all memory of skiplist is allocated from arena. Drawback: we have to copy the content of key/value
//! 2. Tower stores pointer to Node instead of offset, so we can use other arena implementation
//! 3. Use [ArenaSlice] to replace Bytes
//! 4. impl Send/Sync for the iterator

#![allow(non_snake_case)]
mod key;
mod list;
mod slice;

const MAX_HEIGHT: usize = 20;

pub use key::{FixedLengthSuffixComparator, KeyComparator};
pub use list::{SkipListIter, SkipList};
pub use slice::ArenaSlice;
