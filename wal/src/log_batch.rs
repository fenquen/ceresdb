// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Log entries definition.

use std::fmt::Debug;

use bytes_ext::{Buf, BufMut};
use common_types::{table::TableId, SequenceNumber};

use crate::manager::WalLocation;

pub trait Payload: Send + Sync + Debug {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Compute size of the encoded payload.
    fn encode_size(&self) -> usize;
    /// Append the encoded payload to the `buf`.
    fn encode_to<B: BufMut>(&self, buf: &mut B) -> Result<(), Self::Error>;
}

#[derive(Debug)]
pub struct LogEntry<P> {
    pub table_id: TableId,
    pub sequence: SequenceNumber,
    pub payload: P,
}

/// An encoded entry to be written into the Wal.
#[derive(Debug)]
pub struct LogWriteEntry {
    pub payload: Vec<u8>,
}

/// A batch of `LogWriteEntry`s.
#[derive(Debug)]
pub struct LogWriteBatch {
    pub(crate) walLocation: WalLocation,
    pub(crate) logWriteEntryVec: Vec<LogWriteEntry>,
}

impl LogWriteBatch {
    pub fn new(location: WalLocation) -> Self {
        Self::with_capacity(location, 0)
    }

    pub fn with_capacity(location: WalLocation, cap: usize) -> Self {
        Self {
            walLocation: location,
            logWriteEntryVec: Vec::with_capacity(cap),
        }
    }

    #[inline]
    pub fn push(&mut self, entry: LogWriteEntry) {
        self.logWriteEntryVec.push(entry)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.logWriteEntryVec.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.logWriteEntryVec.is_empty()
    }

    #[inline]
    pub fn clear(&mut self) {
        self.logWriteEntryVec.clear()
    }
}

pub trait PayloadDecoder: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    type Target: Send + Sync;
    /// Decode `Target` from the `bytes`.
    fn decode<B: Buf>(&self, buf: &mut B) -> Result<Self::Target, Self::Error>;
}
