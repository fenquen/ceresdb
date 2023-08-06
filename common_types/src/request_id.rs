// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Request id.

use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Clone, Copy)]
pub struct RequestId(u64);

impl RequestId {
    /// Acquire next request id.
    pub fn next_id() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);

        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        Self(id)
    }

    #[inline]
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for RequestId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}