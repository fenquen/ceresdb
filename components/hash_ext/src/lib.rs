// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

/// Which Hash to use:
/// - Memory: aHash
/// - Disk: SeaHash
/// https://github.com/CeresDB/hash-benchmark-rs
use std::hash::BuildHasher;

pub use ahash;
use byteorder::{ByteOrder, LittleEndian};
use murmur3::murmur3_x64_128;
use seahash::SeaHasher;

#[derive(Debug)]
pub struct SeaHasherBuilder;

impl BuildHasher for SeaHasherBuilder {
    type Hasher = SeaHasher;

    fn build_hasher(&self) -> Self::Hasher {
        SeaHasher::new()
    }
}

pub fn hash64(mut bytes: &[u8]) -> u64 {
    let mut out = [0; 16];
    murmur3_x64_128(&mut bytes, 0, &mut out);
    // in most cases we run on little endian target
    LittleEndian::read_u64(&out[0..8])
}

pub fn build_fixed_seed_ahasher_builder() -> ahash::RandomState {
    ahash::RandomState::with_seeds(0, 0, 0, 0)
}