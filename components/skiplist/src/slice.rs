// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Slice with arena

use std::{fmt, ops::Deref, slice};

use arena::{Arena, BasicStats};

/// arena slice
///
/// A slice allocated from the arena, it will holds the reference to the arena so it is safe to clone and deref the slice
#[derive(Clone)]
pub struct ArenaSlice<A: Arena<Stats=BasicStats>> {
    /// arena the slice memory allocated from.
    belongingArena: A,
    /// the slice pointer.
    slicePtr: *const u8,
    /// the slice len.
    sliceLen: usize,
}

impl<A: Arena<Stats=BasicStats>> ArenaSlice<A> {
    /// see the documentation of [`slice::from_raw_parts`] for slice safety requirement
    pub(crate) unsafe fn buildFromSlice(belongingArena: A, slicePtr: *const u8, sliceLen: usize) -> Self {
        Self {
            belongingArena,
            slicePtr,
            sliceLen,
        }
    }
}

unsafe impl<A: Arena<Stats=BasicStats> + Send> Send for ArenaSlice<A> {}

unsafe impl<A: Arena<Stats=BasicStats> + Sync> Sync for ArenaSlice<A> {}

impl<A: Arena<Stats=BasicStats>> Deref for ArenaSlice<A> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.slicePtr, self.sliceLen) }
    }
}

impl<A: Arena<Stats=BasicStats>> fmt::Debug for ArenaSlice<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}