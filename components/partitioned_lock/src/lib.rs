// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partitioned locks

use std::{
    hash::{BuildHasher, Hash, Hasher},
    sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

/// Simple partitioned `RwLock`
pub struct PartitionedRwLock<T, B>
    where
        B: BuildHasher,
{
    partitions: Vec<RwLock<T>>,
    partition_mask: usize,
    hash_builder: B,
}

impl<T, B> PartitionedRwLock<T, B>
    where
        B: BuildHasher,
{
    pub fn try_new<F, E>(init_fn: F, partition_bit: usize, hash_builder: B) -> Result<Self, E>
        where
            F: Fn(usize) -> Result<T, E>,
    {
        let partition_num = 1 << partition_bit;
        let partitions = (1..partition_num)
            .map(|_| init_fn(partition_num).map(RwLock::new))
            .collect::<Result<Vec<RwLock<T>>, E>>()?;

        Ok(Self {
            partitions,
            partition_mask: partition_num - 1,
            hash_builder,
        })
    }

    pub fn read<K: Eq + Hash>(&self, key: &K) -> RwLockReadGuard<'_, T> {
        let rwlock = self.get_partition(key);

        rwlock.read().unwrap()
    }

    pub fn write<K: Eq + Hash>(&self, key: &K) -> RwLockWriteGuard<'_, T> {
        let rwlock = self.get_partition(key);

        rwlock.write().unwrap()
    }

    fn get_partition<K: Eq + Hash>(&self, key: &K) -> &RwLock<T> {
        let mut hasher = self.hash_builder.build_hasher();

        key.hash(&mut hasher);

        &self.partitions[(hasher.finish() as usize) & self.partition_mask]
    }

    #[cfg(test)]
    fn get_partition_by_index(&self, index: usize) -> &RwLock<T> {
        &self.partitions[index]
    }
}

/// Simple partitioned `Mutex`
#[derive(Debug)]
pub struct PartitionedMutex<T, B> where B: BuildHasher {
    partitions: Vec<Mutex<T>>,
    partition_mask: usize,
    hash_builder: B,
}

impl<T, B> PartitionedMutex<T, B> where B: BuildHasher {
    pub fn try_new<F, E>(init_fn: F, partition_bit: usize, hash_builder: B) -> Result<Self, E> where F: Fn(usize) -> Result<T, E> {
        let partition_num = 1 << partition_bit;
        let partitions = (0..partition_num)
            .map(|_| init_fn(partition_num).map(Mutex::new))
            .collect::<Result<Vec<Mutex<T>>, E>>()?;

        Ok(Self {
            partitions,
            partition_mask: partition_num - 1,
            hash_builder,
        })
    }

    pub fn lock<K: Eq + Hash>(&self, key: &K) -> MutexGuard<'_, T> {
        let mutex = self.get_partition(key);

        mutex.lock().unwrap()
    }

    fn get_partition<K: Eq + Hash>(&self, key: &K) -> &Mutex<T> {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        &self.partitions[(hasher.finish() as usize) & self.partition_mask]
    }

    /// This function should be marked with `#[cfg(test)]`, but there is [an issue](https://github.com/rust-lang/cargo/issues/8379) in cargo, so public this function now.
    pub fn get_all_partition(&self) -> &Vec<Mutex<T>> {
        &self.partitions
    }
}

#[derive(Debug)]
pub struct PartitionedMutexAsync<T, B>
    where
        B: BuildHasher,
{
    partitions: Vec<tokio::sync::Mutex<T>>,
    partition_mask: usize,
    hash_builder: B,
}

impl<T, B> PartitionedMutexAsync<T, B>
    where
        B: BuildHasher,
{
    pub fn try_new<F, E>(init_fn: F, partition_bit: usize, hash_builder: B) -> Result<Self, E>
        where
            F: Fn(usize) -> Result<T, E>,
    {
        let partition_num = 1 << partition_bit;
        let partitions = (0..partition_num)
            .map(|_| init_fn(partition_num).map(tokio::sync::Mutex::new))
            .collect::<Result<Vec<tokio::sync::Mutex<T>>, E>>()?;

        Ok(Self {
            partitions,
            partition_mask: partition_num - 1,
            hash_builder,
        })
    }

    pub async fn lock<K: Eq + Hash>(&self, key: &K) -> tokio::sync::MutexGuard<'_, T> {
        let mutex = self.get_partition(key);

        mutex.lock().await
    }

    fn get_partition<K: Eq + Hash>(&self, key: &K) -> &tokio::sync::Mutex<T> {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        &self.partitions[(hasher.finish() as usize) & self.partition_mask]
    }
}