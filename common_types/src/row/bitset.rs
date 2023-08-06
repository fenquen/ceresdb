// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Simple BitSet implementation.

#![allow(dead_code)]

const BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
const UNSET_BIT_MASK: [u8; 8] = [
    255 - 1,
    255 - 2,
    255 - 4,
    255 - 8,
    255 - 16,
    255 - 32,
    255 - 64,
    255 - 128,
];

/// A basic implementation supporting read/write.
#[derive(Debug, Default, Clone)]
pub struct BitSet {
    /// The bits are stored as bytes in the least significant bit order.
    buffer: Vec<u8>,
    /// The number of real bits in the `buffer`
    num_bits: usize,
}

impl BitSet {
    /// Initialize a unset [`BitSet`].
    pub fn new(num_bits: usize) -> Self {
        Self {
            buffer: vec![0; Self::num_bytes(num_bits)],
            num_bits,
        }
    }

    /// Initialize a [`BitSet`] with all bits set.
    pub fn all_set(num_bits: usize) -> Self {
        Self {
            buffer: vec![0xFF; Self::num_bytes(num_bits)],
            num_bits,
        }
    }

    #[inline]
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    #[inline]
    pub fn num_bytes(num_bits: usize) -> usize {
        (num_bits + 7) >> 3
    }

    /// Initialize directly from a buffer.
    ///
    /// None will be returned if the buffer's length is not enough to cover the
    /// bits of `num_bits`.
    pub fn try_from_raw(buffer: Vec<u8>, num_bits: usize) -> Option<Self> {
        if buffer.len() < Self::num_bytes(num_bits) {
            None
        } else {
            Some(Self { buffer, num_bits })
        }
    }

    /// Set the bit at the `index`.
    ///
    /// Return false if the index is outside the range.
    pub fn set(&mut self, index: usize) -> bool {
        if index >= self.num_bits {
            return false;
        }
        let (byte_index, bit_index) = RoBitSet::compute_byte_bit_index(index);
        self.buffer[byte_index] |= BIT_MASK[bit_index];
        true
    }

    /// Set the bit at the `index`.
    ///
    /// Return false if the index is outside the range.
    pub fn unset(&mut self, index: usize) -> bool {
        if index >= self.num_bits {
            return false;
        }
        let (byte_index, bit_index) = RoBitSet::compute_byte_bit_index(index);
        self.buffer[byte_index] &= UNSET_BIT_MASK[bit_index];
        true
    }

    /// Tells whether the bit at the `index` is set.
    pub fn is_set(&self, index: usize) -> Option<bool> {
        let ro = RoBitSet {
            buffer: &self.buffer,
            num_bits: self.num_bits,
        };
        ro.is_set(index)
    }

    /// Tells whether the bit at the `index` is unset.
    pub fn is_unset(&self, index: usize) -> Option<bool> {
        let ro = RoBitSet {
            buffer: &self.buffer,
            num_bits: self.num_bits,
        };
        ro.is_unset(index)
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buffer
    }
}

/// A readonly version of [BitSet], only supports read.
pub struct RoBitSet<'a> {
    /// The bits are stored as bytes in the least significant bit order.
    buffer: &'a [u8],
    /// The number of real bits in the `buffer`
    num_bits: usize,
}

impl<'a> RoBitSet<'a> {
    pub fn try_new(buffer: &'a [u8], num_bits: usize) -> Option<Self> {
        if buffer.len() < BitSet::num_bytes(num_bits) {
            None
        } else {
            Some(Self { buffer, num_bits })
        }
    }

    /// Tells whether the bit at the `index` is set.
    pub fn is_set(&self, index: usize) -> Option<bool> {
        if index >= self.num_bits {
            return None;
        }
        let (byte_index, bit_index) = Self::compute_byte_bit_index(index);
        let set = (self.buffer[byte_index] & (1 << bit_index)) != 0;
        Some(set)
    }

    /// Tells whether the bit at the `index` is set.
    #[inline]
    pub fn is_unset(&self, index: usize) -> Option<bool> {
        self.is_set(index).map(|v| !v)
    }

    #[inline]
    fn compute_byte_bit_index(index: usize) -> (usize, usize) {
        (index >> 3, index & 7)
    }
}
