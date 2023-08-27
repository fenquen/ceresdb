// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Memtable key
//!
//! Some concepts:
//! - User key (row key) is a bytes encoded from the key columns of a row
//! - Internal key contains
//!     - user key
//!     - memtable key sequence
//!         - sequence number
//!         - index

use std::mem;

use bytes_ext::{BufMut, BytesMut, SafeBuf, SafeBufMut};
use codec::{memcomparable::MemComparable, Decoder, Encoder};
use common_types::{row::Row, schema::Schema, SequenceNumber};
use macros::define_result;
use snafu::{ensure, Backtrace, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to encode key datum, err:{}", source))]
    EncodeKeyDatum { source: codec::memcomparable::Error },

    #[snafu(display("Failed to encode sequence, err:{}", source))]
    EncodeSequence { source: bytes_ext::Error },

    #[snafu(display("Failed to encode row index, err:{}", source))]
    EncodeIndex { source: bytes_ext::Error },

    #[snafu(display("Failed to decode sequence, err:{}", source))]
    DecodeSequence { source: bytes_ext::Error },

    #[snafu(display("Failed to decode row index, err:{}", source))]
    DecodeIndex { source: bytes_ext::Error },

    #[snafu(display(
    "Insufficient internal key length, len:{}.\nBacktrace:\n{}",
    len,
    backtrace
    ))]
    InternalKeyLen { len: usize, backtrace: Backtrace },
}

define_result!(Error);

// 8个字节 sequenceNumber 4个字节 rowindex
const KEY_SEQUENCE_BYTES_LEN: usize = 12;

/// Row index in the batch
pub type RowIndex = u32;

/// Sequence number of row in memtable
///
/// Contains:
/// - sequence number in wal (sequence number of the write batch)
/// - unique index of the row in the write batch
///
/// Ordering:
/// 1. ordered by sequence desc
/// 2. ordered by index desc
///
/// The desc order is implemented via MAX - seq
///
/// The index is used to distinguish rows with same key of the same write batch
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeySequence(SequenceNumber, RowIndex);

impl KeySequence {
    pub fn new(sequence: SequenceNumber, index: RowIndex) -> Self {
        Self(sequence, index)
    }

    #[inline]
    pub fn sequence(&self) -> SequenceNumber {
        self.0
    }

    #[inline]
    pub fn row_index(&self) -> RowIndex {
        self.1
    }
}

// TODO(yingwen): We also need opcode (PUT/DELETE), put it in key or row value
/// Comparable internal key encoder
///
/// Key order:
/// 1. ordered by user key ascend (key parts of a row)
/// 2. ordered by sequence descend
///
/// Encoding:
/// user_key + sequence
///
/// REQUIRE: The schema of row to encode matches the Self::schema
pub struct ComparableInternalKey<'a> {
    /// Sequence number of the row
    keySequence: KeySequence,
    /// Schema of row
    schema: &'a Schema,
}

impl<'a> ComparableInternalKey<'a> {
    pub fn new(sequence: KeySequence, schema: &'a Schema) -> Self {
        Self { keySequence: sequence, schema }
    }
}

impl<'a> Encoder<Row> for ComparableInternalKey<'a> {
    type Error = Error;

    fn encode<B: BufMut>(&self, buf: &mut B, row: &Row) -> Result<()> {
        let encoder = MemComparable;

        for idx in self.schema.primary_key_indexes() {
            encoder.encode(buf, &row[*idx]).context(EncodeKeyDatum)?;
        }

        KeySequenceCodec.encode(buf, &self.keySequence)?;

        Ok(())
    }

    fn estimateEncodedSize(&self, value: &Row) -> usize {
        let encoder = MemComparable;
        let mut total_len = 0;
        for idx in self.schema.primary_key_indexes() {
            total_len += encoder.estimateEncodedSize(&value[*idx]);
        }
        total_len += KEY_SEQUENCE_BYTES_LEN;

        total_len
    }
}

struct KeySequenceCodec;

impl Encoder<KeySequence> for KeySequenceCodec {
    type Error = Error;

    fn encode<B: BufMut>(&self, buf: &mut B, keySequence: &KeySequence) -> Result<()> {
        // Encode sequence number and index in descend order
        encode_sequence_number(buf, keySequence.sequence())?;
        let reversed_index = RowIndex::MAX - keySequence.row_index();
        buf.try_put_u32(reversed_index).context(EncodeIndex)?;

        Ok(())
    }

    fn estimateEncodedSize(&self, _value: &KeySequence) -> usize {
        KEY_SEQUENCE_BYTES_LEN
    }
}

impl Decoder<KeySequence> for KeySequenceCodec {
    type Error = Error;

    fn decode<B: SafeBuf>(&self, buf: &mut B) -> Result<KeySequence> {
        // Reverse sequence
        let sequence = buf.try_get_u64().context(DecodeSequence)?;
        let sequence = SequenceNumber::MAX - sequence;

        // Reverse row index
        let row_index = buf.try_get_u32().context(DecodeIndex)?;
        let row_index = RowIndex::MAX - row_index;

        Ok(KeySequence::new(sequence, row_index))
    }
}

// TODO(yingwen): Maybe make decoded internal key a type?

/// userKey sequence
pub fn buildSeekKey<'a>(user_key: &[u8],
                        sequence: u64,
                        scratch: &'a mut BytesMut) -> Result<&'a [u8]> {
    scratch.clear();
    scratch.reserve(user_key.len() + mem::size_of::<SequenceNumber>());
    scratch.extend_from_slice(user_key);

    encode_sequence_number(scratch, sequence)?;

    Ok(&scratch[..])
}

#[inline]
fn encode_sequence_number<B: SafeBufMut>(buf: &mut B, sequence: SequenceNumber) -> Result<()> {
    // the sequence need to encode in descend order
    let reversed_sequence = SequenceNumber::MAX - sequence;

    // encode sequence
    buf.try_put_u64(reversed_sequence).context(EncodeSequence)?;
    Ok(())
}

/// decode user key and sequence number from the internal key
pub fn user_key_from_internal_key(internal_key: &[u8]) -> Result<(&[u8], KeySequence)> {
    // empty user key is meaningless
    ensure!(internal_key.len() > KEY_SEQUENCE_BYTES_LEN, InternalKeyLen {len: internal_key.len()});

    let (left, mut right) = internal_key.split_at(internal_key.len() - KEY_SEQUENCE_BYTES_LEN);

    // decode sequence number from right part
    let sequence = KeySequenceCodec.decode(&mut right)?;

    Ok((left, sequence))
}