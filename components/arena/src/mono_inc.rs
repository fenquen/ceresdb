// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    alloc::{alloc, dealloc, Layout},
    ptr::NonNull,
    sync::Arc,
};

use parking_lot::Mutex;

use crate::arena_trait::{Arena, BasicStats, Collector, CollectorRef};

/// The noop collector does nothing on alloc and free
pub struct NoopCollector;

impl Collector for NoopCollector {
    fn on_alloc(&self, _bytes: usize) {}

    fn on_used(&self, _bytes: usize) {}

    fn on_free(&self, _used: usize, _allocated: usize) {}
}

const DEFAULT_ALIGN: usize = 8;

/// A thread-safe arena. All allocated memory is aligned to 8. Organizes its
/// allocated memory as blocks.
#[derive(Clone)]
pub struct MonoIncArena {
    core: Arc<Mutex<ArenaCore>>,
}

impl MonoIncArena {
    pub fn new(regular_block_size: usize) -> Self {
        Self {
            core: Arc::new(Mutex::new(ArenaCore::new(
                regular_block_size,
                Arc::new(NoopCollector {}),
            ))),
        }
    }

    pub fn with_collector(regular_block_size: usize, collector: CollectorRef) -> Self {
        Self {
            core: Arc::new(Mutex::new(ArenaCore::new(regular_block_size, collector))),
        }
    }
}

impl Arena for MonoIncArena {
    type Stats = BasicStats;

    fn try_alloc(&self, layout: Layout) -> Option<NonNull<u8>> {
        Some(self.core.lock().alloc(layout))
    }

    fn stats(&self) -> Self::Stats {
        self.core.lock().stats
    }

    fn alloc(&self, layout: Layout) -> NonNull<u8> {
        self.core.lock().alloc(layout)
    }
}

struct ArenaCore {
    collector: CollectorRef,
    regular_layout: Layout,
    regular_blocks: Vec<Block>,
    special_blocks: Vec<Block>,
    stats: BasicStats,
}

impl ArenaCore {
    /// # Safety
    /// Required property is tested in debug assertions.
    fn new(regular_block_size: usize, collector: CollectorRef) -> Self {
        debug_assert_ne!(DEFAULT_ALIGN, 0);
        debug_assert_eq!(DEFAULT_ALIGN & (DEFAULT_ALIGN - 1), 0);
        // TODO(yingwen): Avoid panic.
        let regular_layout = Layout::from_size_align(regular_block_size, DEFAULT_ALIGN).unwrap();
        let regular_blocks = vec![Block::new(regular_layout)];
        let special_blocks = vec![];
        let bytes = regular_layout.size();
        collector.on_alloc(bytes);

        Self {
            collector,
            regular_layout,
            regular_blocks,
            special_blocks,
            stats: BasicStats {
                bytes_allocated: bytes,
                bytes_used: 0,
            },
        }
    }

    /// Input layout will be aligned.
    fn alloc(&mut self, layout: Layout) -> NonNull<u8> {
        let layout = layout
            .align_to(self.regular_layout.align())
            .unwrap()
            .pad_to_align();
        let bytes = layout.size();
        // TODO(Ruihang): determine threshold
        if layout.size() > self.regular_layout.size() {
            self.stats.bytes_used += bytes;
            self.collector.on_used(bytes);
            Self::add_new_block(
                layout,
                &mut self.special_blocks,
                &mut self.stats,
                &self.collector,
            );
            let block = self.special_blocks.last().unwrap();
            return block.data;
        }

        self.stats.bytes_used += bytes;
        self.collector.on_used(bytes);
        if let Some(ptr) = self.try_alloc(layout) {
            ptr
        } else {
            Self::add_new_block(
                self.regular_layout,
                &mut self.regular_blocks,
                &mut self.stats,
                &self.collector,
            );
            self.try_alloc(layout).unwrap()
        }
    }

    /// # Safety
    /// `regular_blocks` vector is guaranteed to contains at least one element.
    fn try_alloc(&mut self, layout: Layout) -> Option<NonNull<u8>> {
        self.regular_blocks.last_mut().unwrap().alloc(layout)
    }

    fn add_new_block(
        layout: Layout,
        container: &mut Vec<Block>,
        stats: &mut BasicStats,
        collector: &CollectorRef,
    ) {
        let new_block = Block::new(layout);
        container.push(new_block);
        // Update allocated stats once a new block has been allocated from the system.
        stats.bytes_allocated += layout.size();
        collector.on_alloc(layout.size());
    }
}

impl Drop for ArenaCore {
    fn drop(&mut self) {
        self.collector
            .on_free(self.stats.bytes_used, self.stats.bytes_allocated);
    }
}

struct Block {
    data: NonNull<u8>,
    len: usize,
    layout: Layout,
}

impl Block {
    /// Create a new block. Return the pointer of this new block.
    ///
    /// # Safety
    /// See [std::alloc::alloc]. The allocated memory will be deallocated in
    /// drop().
    fn new(layout: Layout) -> Block {
        let data = unsafe { alloc(layout) };

        Self {
            data: NonNull::new(data).unwrap(),
            len: 0,
            layout,
        }
    }

    /// # Safety
    /// ## ptr:add()
    /// The added offset is checked before.
    /// ## NonNull::new_unchecked()
    /// `ptr` is added from a NonNull.
    fn alloc(&mut self, layout: Layout) -> Option<NonNull<u8>> {
        let size = layout.size();

        if self.len + size <= self.layout.size() {
            let ptr = unsafe { self.data.as_ptr().add(self.len) };
            self.len += size;
            unsafe { Some(NonNull::new_unchecked(ptr)) }
        } else {
            None
        }
    }
}

impl Drop for Block {
    /// Reclaim space pointed by `data`.
    fn drop(&mut self) {
        unsafe { dealloc(self.data.as_ptr(), self.layout) }
    }
}

unsafe impl Send for Block {}
unsafe impl Sync for Block {}