// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! hotspot LRU
use std::{hash::Hash, num::NonZeroUsize};

use clru::CLruCache;

pub struct HotspotLru<K> {
    heats: CLruCache<K, u64>,
}

impl<K: Hash + Eq + Clone> HotspotLru<K> {
    /// Creates a new LRU Hotspot that holds at most `cap` items
    pub fn new(cap: usize) -> Option<HotspotLru<K>> {
        NonZeroUsize::new(cap).map(|cap| Self {
            heats: CLruCache::new(cap),
        })
    }

    /// Incs heat into hotspot cache, If the key already exists it
    /// updates its heat value
    pub fn inc(&mut self, key: &K, heat: u64) {
        match self.heats.get_mut(key) {
            Some(val) => *val += heat,
            None => {
                self.heats.put(key.clone(), heat);
            }
        }
    }

    /// Removes and returns all items.
    pub fn pop_all(&mut self) -> Vec<(K, u64)> {
        let mut values = Vec::with_capacity(self.heats.len());

        while let Some(value) = self.heats.pop_back() {
            values.push(value);
        }

        self.heats.clear();
        values
    }
}