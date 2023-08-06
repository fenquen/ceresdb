// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Slice with arena

use std::{fmt, ops::Deref, slice};

use arena::{Arena, BasicStats};

/// Arena slice
///
/// A slice allocated from the arena, it will holds the reference to the arena
/// so it is safe to clone and deref the slice
#[derive(Clone)]
pub struct ArenaSlice<A: Arena<Stats = BasicStats>> {
    /// Arena the slice memory allocated from.
    _arena: A,
    /// The slice pointer.
    slice_ptr: *const u8,
    /// The slice len.
    slice_len: usize,
}

impl<A: Arena<Stats = BasicStats>> ArenaSlice<A> {
    /// Create a [ArenaSlice]
    ///
    /// See the documentation of [`slice::from_raw_parts`] for slice safety
    /// requirements.
    pub(crate) unsafe fn from_raw_parts(_arena: A, slice_ptr: *const u8, slice_len: usize) -> Self {
        Self {
            _arena,
            slice_ptr,
            slice_len,
        }
    }
}

unsafe impl<A: Arena<Stats = BasicStats> + Send> Send for ArenaSlice<A> {}
unsafe impl<A: Arena<Stats = BasicStats> + Sync> Sync for ArenaSlice<A> {}

impl<A: Arena<Stats = BasicStats>> Deref for ArenaSlice<A> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.slice_ptr, self.slice_len) }
    }
}

impl<A: Arena<Stats = BasicStats>> fmt::Debug for ArenaSlice<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}