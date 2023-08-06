// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Alloc tracker

use std::sync::atomic::{AtomicUsize, Ordering};

/// Collect memory usage from tracker, useful for extending the tracker
pub trait Collector {
    /// Called when `bytes` bytes memory is allocated and tracked by the tracker
    fn on_allocate(&self, bytes: usize);

    /// Called when `bytes` bytes memory is freed and tracked by the tracker
    fn on_free(&self, bytes: usize);
}

/// A tracker to track memory in used
// TODO(yingwen): Impl a thread local or local tracker that are not thread safe,
// and collect statistics into the thread safe one for better performance
pub struct Tracker<T: Collector> {
    collector: T,
    bytes_allocated: AtomicUsize,
}

impl<T: Collector> Tracker<T> {
    pub fn new(collector: T) -> Self {
        Self {
            collector,
            bytes_allocated: AtomicUsize::new(0),
        }
    }

    /// Increase consumption of this tracker by bytes
    pub fn consume(&self, bytes: usize) {
        self.bytes_allocated.fetch_add(bytes, Ordering::Relaxed);
        self.collector.on_allocate(bytes);
    }

    /// Decrease consumption of this tracker by bytes
    ///
    /// The caller should guarantee the released bytes wont larger than bytes
    /// already consumed
    pub fn release(&self, bytes: usize) {
        self.bytes_allocated.fetch_sub(bytes, Ordering::Relaxed);
        self.collector.on_free(bytes);
    }

    /// Bytes allocated
    pub fn bytes_allocated(&self) -> usize {
        self.bytes_allocated.load(Ordering::Relaxed)
    }
}

impl<T: Collector> Drop for Tracker<T> {
    fn drop(&mut self) {
        let bytes = *self.bytes_allocated.get_mut();
        self.collector.on_free(bytes);
    }
}

/// The noop collector does nothing on alloc and free
struct NoopCollector;

impl Collector for NoopCollector {
    fn on_allocate(&self, _bytes: usize) {}

    fn on_free(&self, _bytes: usize) {}
}

/// A simple tracker hides the collector api
pub struct SimpleTracker(Tracker<NoopCollector>);

impl Default for SimpleTracker {
    fn default() -> Self {
        Self(Tracker::new(NoopCollector))
    }
}

impl SimpleTracker {
    /// Increase consumption of this tracker by bytes
    #[inline]
    pub fn consume(&self, bytes: usize) {
        self.0.consume(bytes);
    }

    /// Decrease consumption of this tracker by bytes
    ///
    /// The caller should guarantee the released bytes wont larger than bytes
    /// already consumed
    #[inline]
    pub fn release(&self, bytes: usize) {
        self.0.release(bytes);
    }

    /// Bytes allocated
    pub fn bytes_allocated(&self) -> usize {
        self.0.bytes_allocated()
    }
}