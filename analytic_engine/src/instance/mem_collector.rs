// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};

use arena::{Collector, CollectorRef};

/// Space memtable memory usage collector
pub struct MemUsageCollector {
    /// Memory size allocated in bytes.
    bytes_allocated: AtomicUsize,
    /// Memory size used in bytes.
    bytes_used: AtomicUsize,
    parent: Option<CollectorRef>,
}

impl Collector for MemUsageCollector {
    fn on_alloc(&self, bytes: usize) {
        self.bytes_allocated.fetch_add(bytes, Ordering::Relaxed);

        if let Some(c) = &self.parent {
            c.on_alloc(bytes);
        }
    }

    fn on_used(&self, bytes: usize) {
        self.bytes_used.fetch_add(bytes, Ordering::Relaxed);

        if let Some(c) = &self.parent {
            c.on_used(bytes);
        }
    }

    fn on_free(&self, used: usize, allocated: usize) {
        self.bytes_allocated.fetch_sub(allocated, Ordering::Relaxed);
        self.bytes_used.fetch_sub(used, Ordering::Relaxed);

        if let Some(c) = &self.parent {
            c.on_free(used, allocated);
        }
    }
}

impl Default for MemUsageCollector {
    fn default() -> Self {
        Self {
            bytes_allocated: AtomicUsize::new(0),
            bytes_used: AtomicUsize::new(0),
            parent: None,
        }
    }
}

impl MemUsageCollector {
    pub fn with_parent(collector: CollectorRef) -> Self {
        Self {
            bytes_allocated: AtomicUsize::new(0),
            bytes_used: AtomicUsize::new(0),
            parent: Some(collector),
        }
    }

    #[inline]
    pub fn total_memory_allocated(&self) -> usize {
        self.bytes_allocated.load(Ordering::Relaxed)
    }
}