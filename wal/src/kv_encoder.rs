// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Common Encoding for Wal logs

use bytes_ext::{self, Buf, BufMut, BytesMut, SafeBuf, SafeBufMut};
use codec::{Decoder, Encoder};
use common_types::{table::TableId, SequenceNumber};
use generic_error::{BoxError, GenericError};
use macros::define_result;
use snafu::{ensure, Backtrace, ResultExt, Snafu};

use crate::{
    log_batch::{LogWriteBatch, LogWriteEntry, Payload},
    manager::{self, Encoding, WalLocation},
};

pub const LOG_KEY_ENCODING_V0: u8 = 0;
pub const NEWEST_LOG_KEY_ENCODING_VERSION: u8 = LOG_KEY_ENCODING_V0;

pub const LOG_VALUE_ENCODING_V0: u8 = 0;
pub const NEWEST_LOG_VALUE_ENCODING_VERSION: u8 = LOG_VALUE_ENCODING_V0;

pub const META_KEY_ENCODING_V0: u8 = 0;
pub const NEWEST_META_KEY_ENCODING_VERSION: u8 = META_KEY_ENCODING_V0;

pub const META_VALUE_ENCODING_V0: u8 = 0;
pub const NEWEST_META_VALUE_ENCODING_VERSION: u8 = META_VALUE_ENCODING_V0;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to encode log key, err:{}", source))]
    EncodeLogKey {
        source: bytes_ext::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to encode log value header, err:{}", source))]
    EncodeLogValueHeader { source: bytes_ext::Error },

    #[snafu(display("Failed to encode log value payload, err:{}", source))]
    EncodeLogValuePayload { source: GenericError },

    #[snafu(display("Failed to decode log key, err:{}", source))]
    DecodeLogKey { source: bytes_ext::Error },

    #[snafu(display("Failed to decode log value header, err:{}", source))]
    DecodeLogValueHeader { source: bytes_ext::Error },

    #[snafu(display("Failed to decode log value payload, err:{}", source))]
    DecodeLogValuePayload { source: GenericError },

    #[snafu(display("Failed to encode meta key, err:{}", source))]
    EncodeMetaKey {
        source: bytes_ext::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to encode meta value, err:{}", source))]
    EncodeMetaValue { source: bytes_ext::Error },

    #[snafu(display("Failed to decode meta key, err:{}", source))]
    DecodeMetaKey { source: bytes_ext::Error },

    #[snafu(display("Failed to decode meta value, err:{}", source))]
    DecodeMetaValue { source: bytes_ext::Error },

    #[snafu(display(
    "Found invalid meta key type, expect:{:?}, given:{}.\nBacktrace:\n{}",
    expect,
    given,
    backtrace
    ))]
    InvalidMetaKeyType {
        expect: MetaKeyType,
        given: u8,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Found invalid namespace, expect:{:?}, given:{}.\nBacktrace:\n{}",
    expect,
    given,
    backtrace
    ))]
    InvalidNamespace {
        expect: Namespace,
        given: u8,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Found invalid version, expect:{}, given:{}.\nBacktrace:\n{}",
    expect,
    given,
    backtrace
    ))]
    InvalidVersion {
        expect: u8,
        given: u8,
        backtrace: Backtrace,
    },
}

define_result!(Error);

#[derive(Debug, Copy, Clone)]
pub enum Namespace {
    Meta = 0,
    Log = 1,
}

/// Log key in old wal design, map the `TableId` to `RegionId`
pub type LogKey = (u64, SequenceNumber);

// fenquen 似乎主要功已然被CommonLogEncoding的CommonLogKeyEncoder
#[derive(Debug, Clone)]
pub struct LogKeyEncoder {
    pub version: u8,
    pub namespace: Namespace,
}

impl LogKeyEncoder {
    /// Create newest version encoder.
    pub fn newest() -> Self {
        Self {
            version: NEWEST_LOG_KEY_ENCODING_VERSION,
            namespace: Namespace::Log,
        }
    }

    /// Determine whether the raw bytes is a log key.
    pub fn is_valid<B: Buf>(&self, buf: &mut B) -> Result<bool> {
        let namespace = buf.try_get_u8().context(DecodeLogKey)?;
        Ok(namespace == self.namespace as u8)
    }
}

impl Encoder<LogKey> for LogKeyEncoder {
    type Error = Error;

    /// Key format:
    ///
    /// ```text
    /// +---------------+----------------+-------------------+--------------------+
    /// | namespace(u8) | region_id(u64) | sequence_num(u64) | version header(u8) |
    /// +---------------+----------------+-------------------+--------------------+
    /// ```
    ///
    /// More information can be extended after the incremented `version header`.
    fn encode<B: BufMut>(&self, buf: &mut B, log_key: &LogKey) -> Result<()> {
        buf.try_put_u8(self.namespace as u8).context(EncodeLogKey)?;
        buf.try_put_u64(log_key.0).context(EncodeLogKey)?;
        buf.try_put_u64(log_key.1).context(EncodeLogKey)?;
        buf.try_put_u8(self.version).context(EncodeLogKey)?;

        Ok(())
    }

    fn estimateEncodedSize(&self, _log_key: &LogKey) -> usize {
        // Refer to key format.
        1 + 8 + 8 + 1
    }
}

impl Decoder<LogKey> for LogKeyEncoder {
    type Error = Error;

    fn decode<B: Buf>(&self, buf: &mut B) -> Result<LogKey> {
        // check namespace
        let namespace = buf.try_get_u8().context(DecodeLogKey)?;
        ensure!(
            namespace == self.namespace as u8,
            InvalidNamespace {
                expect: self.namespace,
                given: namespace
            }
        );

        let log_key = (
            buf.try_get_u64().context(DecodeLogKey)?,
            buf.try_get_u64().context(DecodeLogKey)?,
        );

        // check version
        let version = buf.try_get_u8().context(DecodeLogKey)?;
        ensure!(
            version == self.version,
            InvalidVersion {
                expect: self.version,
                given: version
            }
        );

        Ok(log_key)
    }
}

#[derive(Debug, Clone)]
pub struct LogValueEncoder {
    pub version: u8,
}

impl LogValueEncoder {
    /// Create newest version encoder.
    pub fn newest() -> Self {
        Self {
            version: NEWEST_LOG_VALUE_ENCODING_VERSION,
        }
    }
}

impl<T: Payload> Encoder<T> for LogValueEncoder {
    type Error = Error;

    /// Value format:
    /// +--------------------+---------+
    /// | version_header(u8) | payload |
    /// +--------------------+---------+
    fn encode<B: BufMut>(&self, buf: &mut B, payload: &T) -> Result<()> {
        buf.try_put_u8(self.version).context(EncodeLogValueHeader)?;

        payload
            .encode_to(buf)
            .box_err()
            .context(EncodeLogValuePayload)
    }

    fn estimateEncodedSize(&self, payload: &T) -> usize {
        // Refer to value format.
        1 + payload.encode_size()
    }
}

pub struct LogValueDecoder {
    pub version: u8,
}

impl LogValueDecoder {
    pub fn decode<'a>(&self, mut buf: &'a [u8]) -> Result<&'a [u8]> {
        let version = buf.try_get_u8().context(DecodeLogValueHeader)?;
        ensure!(
            version == self.version,
            InvalidVersion {
                expect: self.version,
                given: version
            }
        );

        Ok(buf)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum MetaKeyType {
    MaxSeq = 0,
}

#[derive(Clone, Debug)]
pub struct MetaKeyEncoder {
    version: u8,
    key_type: MetaKeyType,
    namespace: Namespace,
}

#[derive(Clone, Debug)]
pub struct MetaKey {
    pub table_id: u64,
}

impl MetaKeyEncoder {
    /// Determine whether the raw bytes is a valid meta key.
    pub fn is_valid<B: Buf>(&self, buf: &mut B) -> Result<bool> {
        let namespace = buf.try_get_u8().context(DecodeMetaKey)?;
        let key_type = buf.try_get_u8().context(DecodeMetaKey)?;
        Ok(namespace == self.namespace as u8 && key_type == self.key_type as u8)
    }
}

impl Encoder<MetaKey> for MetaKeyEncoder {
    type Error = Error;

    /// Key format:
    ///
    /// ```text
    /// +---------------+--------------+----------------+--------------------+
    /// | namespace(u8) | key_type(u8) | region_id(u64) | version header(u8) |
    /// +---------------+--------------+----------------+--------------------+
    /// ```
    ///
    /// More information can be extended after the incremented `version header`.
    fn encode<B: BufMut>(&self, buf: &mut B, meta_key: &MetaKey) -> Result<()> {
        buf.try_put_u8(self.namespace as u8)
            .context(EncodeMetaKey)?;
        buf.try_put_u8(self.key_type as u8).context(EncodeMetaKey)?;
        buf.try_put_u64(meta_key.table_id).context(EncodeMetaKey)?;
        buf.try_put_u8(self.version).context(EncodeMetaKey)?;

        Ok(())
    }

    fn estimateEncodedSize(&self, _log_key: &MetaKey) -> usize {
        // Refer to key format.
        1 + 1 + 8 + 1
    }
}

impl Decoder<MetaKey> for MetaKeyEncoder {
    type Error = Error;

    fn decode<B: Buf>(&self, buf: &mut B) -> Result<MetaKey> {
        // check namespace
        let namespace = buf.try_get_u8().context(DecodeMetaKey)?;
        ensure!(
            namespace == self.namespace as u8,
            InvalidNamespace {
                expect: self.namespace,
                given: namespace
            }
        );

        let key_type = buf.try_get_u8().context(DecodeMetaKey)?;
        ensure!(
            key_type == self.key_type as u8,
            InvalidMetaKeyType {
                expect: self.key_type,
                given: key_type,
            }
        );

        let table_id = buf.try_get_u64().context(DecodeMetaKey)?;

        // check version
        let version = buf.try_get_u8().context(DecodeMetaKey)?;
        ensure!(
            version == self.version,
            InvalidVersion {
                expect: self.version,
                given: version
            }
        );

        Ok(MetaKey { table_id })
    }
}

#[derive(Clone, Debug)]
pub struct MaxSeqMetaValue {
    pub max_seq: SequenceNumber,
}

#[derive(Clone, Debug)]
pub struct MaxSeqMetaValueEncoder {
    version: u8,
}

impl Encoder<MaxSeqMetaValue> for MaxSeqMetaValueEncoder {
    type Error = Error;

    /// Value format:
    ///
    /// ```text
    /// +--------------------+--------------+
    /// | version header(u8) | max_seq(u64) |
    /// +--------------------+--------------+
    /// ```
    ///
    /// More information can be extended after the incremented `version header`.
    fn encode<B: BufMut>(&self, buf: &mut B, meta_value: &MaxSeqMetaValue) -> Result<()> {
        buf.try_put_u8(self.version).context(EncodeMetaValue)?;
        buf.try_put_u64(meta_value.max_seq)
            .context(EncodeMetaValue)?;

        Ok(())
    }

    fn estimateEncodedSize(&self, _meta_value: &MaxSeqMetaValue) -> usize {
        // Refer to value format.
        1 + 8
    }
}

impl Decoder<MaxSeqMetaValue> for MaxSeqMetaValueEncoder {
    type Error = Error;

    fn decode<B: Buf>(&self, buf: &mut B) -> Result<MaxSeqMetaValue> {
        // check version
        let version = buf.try_get_u8().context(DecodeMetaValue)?;
        ensure!(
            version == self.version,
            InvalidVersion {
                expect: self.version,
                given: version
            }
        );

        let max_seq = buf.try_get_u64().context(DecodeMetaValue)?;
        Ok(MaxSeqMetaValue { max_seq })
    }
}

#[derive(Clone, Debug)]
pub struct MaxSeqMetaEncoding {
    key_enc: MetaKeyEncoder,
    value_enc: MaxSeqMetaValueEncoder,
}

impl MaxSeqMetaEncoding {
    pub fn newest() -> Self {
        Self {
            key_enc: MetaKeyEncoder {
                version: NEWEST_META_KEY_ENCODING_VERSION,
                key_type: MetaKeyType::MaxSeq,
                namespace: Namespace::Meta,
            },
            value_enc: MaxSeqMetaValueEncoder {
                version: NEWEST_META_VALUE_ENCODING_VERSION,
            },
        }
    }

    pub fn is_max_seq_meta_key(&self, mut buf: &[u8]) -> manager::Result<bool> {
        self.key_enc
            .is_valid(&mut buf)
            .box_err()
            .context(manager::Decoding)
    }

    pub fn encode_key(&self, buf: &mut BytesMut, meta_key: &MetaKey) -> manager::Result<()> {
        buf.clear();
        buf.reserve(self.key_enc.estimateEncodedSize(meta_key));
        self.key_enc
            .encode(buf, meta_key)
            .box_err()
            .context(manager::Encoding)?;

        Ok(())
    }

    pub fn encode_value(
        &self,
        buf: &mut BytesMut,
        meta_value: &MaxSeqMetaValue,
    ) -> manager::Result<()> {
        buf.clear();
        buf.reserve(self.value_enc.estimateEncodedSize(meta_value));
        self.value_enc
            .encode(buf, meta_value)
            .box_err()
            .context(manager::Encoding)
    }

    pub fn decode_key(&self, mut buf: &[u8]) -> manager::Result<MetaKey> {
        self.key_enc
            .decode(&mut buf)
            .box_err()
            .context(manager::Decoding)
    }

    pub fn decode_value(&self, mut buf: &[u8]) -> manager::Result<MaxSeqMetaValue> {
        self.value_enc
            .decode(&mut buf)
            .box_err()
            .context(manager::Decoding)
    }
}

#[derive(Debug, Clone)]
pub struct LogEncoding {
    /// fenquen 使用了CommonLogEncoding中的替换
    logKeyEncoder: LogKeyEncoder,
    logValueEncoder: LogValueEncoder,
    // value decoder is created dynamically from the version,
    value_enc_version: u8,
}

impl LogEncoding {
    pub fn newest() -> Self {
        Self {
            logKeyEncoder: LogKeyEncoder::newest(),
            logValueEncoder: LogValueEncoder::newest(),
            value_enc_version: NEWEST_LOG_VALUE_ENCODING_VERSION,
        }
    }

    /// Encode [LogKey] into `buf` and caller should knows that the keys are
    /// ordered by ([RegionId], [SequenceNum]) so the caller can use this
    /// method to generate min/max key in specific scope(global or in some region).
    pub fn encode_key(&self, buf: &mut BytesMut, log_key: &LogKey) -> Result<()> {
        buf.clear();
        buf.reserve(self.logKeyEncoder.estimateEncodedSize(log_key));
        self.logKeyEncoder.encode(buf, log_key)?;

        Ok(())
    }

    pub fn encode_value(&self, buf: &mut BytesMut, payload: &impl Payload) -> Result<()> {
        buf.clear();
        buf.reserve(self.logValueEncoder.estimateEncodedSize(payload));
        self.logValueEncoder.encode(buf, payload)
    }

    pub fn is_log_key(&self, mut buf: &[u8]) -> Result<bool> {
        self.logKeyEncoder.is_valid(&mut buf)
    }

    pub fn decode_key(&self, mut buf: &[u8]) -> Result<LogKey> {
        self.logKeyEncoder.decode(&mut buf)
    }

    pub fn decode_value<'a>(&self, buf: &'a [u8]) -> Result<&'a [u8]> {
        let value_dec = LogValueDecoder {
            version: self.value_enc_version,
        };

        value_dec.decode(buf)
    }
}

/// LogBatchEncoder which are used to encode specify payloads.
#[derive(Debug)]
pub struct LogBatchEncoder {
    /// LogBatchEncoder的walLocation 传递到生成的LogWriteBatch 会参与成为会在的各个条目的key的 fenquen
    walLocation: WalLocation,
    logEncoding: LogEncoding,
}

impl LogBatchEncoder {
    /// Create LogBatchEncoder with specific region_id.
    pub fn create(location: WalLocation) -> Self {
        Self {
            walLocation: location,
            logEncoding: LogEncoding::newest(),
        }
    }

    /// Consume LogBatchEncoder and encode single payload to LogWriteBatch.
    pub fn encode(self, payload: &impl Payload) -> manager::Result<LogWriteBatch> {
        let mut logWriteBatch = LogWriteBatch::new(self.walLocation);

        let mut buf = BytesMut::new();
        self.logEncoding.encode_value(&mut buf, payload).box_err().context(Encoding)?;

        logWriteBatch.push(LogWriteEntry {
            payload: buf.to_vec(),
        });

        Ok(logWriteBatch)
    }

    /// Consume LogBatchEncoder and encode raw payload batch to LogWriteBatch.
    /// Note: To build payload from raw payload in `encode_batch`, raw payload
    /// need implement From trait.
    pub fn encode_batch<'a, P: Payload, I>(self, raw_payload_batch: &'a [I]) -> manager::Result<LogWriteBatch> where &'a I: Into<P>, {
        let mut write_batch = LogWriteBatch::new(self.walLocation);
        let mut buf = BytesMut::new();
        for raw_payload in raw_payload_batch.iter() {
            self.logEncoding
                .encode_value(&mut buf, &raw_payload.into())
                .box_err()
                .context(Encoding)?;

            write_batch.push(LogWriteEntry {
                payload: buf.to_vec(),
            });
        }

        Ok(write_batch)
    }
}

/// Common log key used in multiple wal implementation
#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct CommonLogKey {
    /// region may be mapped to table itself, shard, or others...
    pub region_id: u64,
    pub table_id: TableId,
    pub sequence_num: SequenceNumber,
}

impl CommonLogKey {
    pub fn new(region_id: u64, table_id: TableId, sequence_num: SequenceNumber) -> Self {
        Self { region_id, table_id, sequence_num }
    }
}

#[derive(Debug, Clone)]
pub struct CommonLogKeyEncoder {
    pub version: u8,
    pub namespace: Namespace,
}

impl CommonLogKeyEncoder {
    /// Create newest version encoder.
    pub fn newest() -> Self {
        Self {
            version: NEWEST_LOG_KEY_ENCODING_VERSION,
            namespace: Namespace::Log,
        }
    }

    /// Determine whether the raw bytes is a log key.
    pub fn is_valid<B: Buf>(&self, buf: &mut B) -> Result<bool> {
        let namespace = buf.try_get_u8().context(DecodeLogKey)?;
        Ok(namespace == self.namespace as u8)
    }
}

impl Encoder<CommonLogKey> for CommonLogKeyEncoder {
    type Error = Error;

    /// Key format:
    ///
    /// ```text
    /// +---------------+----------------+---------------+-------------------+--------------------+
    /// | namespace(u8) | region_id(u64) | table_id(u64) | sequence_num(u64) | version header(u8) |
    /// +---------------+----------------+---------------+-------------------+--------------------+
    /// ```
    ///
    /// More information can be extended after the incremented `version header`.
    fn encode<B: BufMut>(&self, buf: &mut B, log_key: &CommonLogKey) -> Result<()> {
        buf.try_put_u8(self.namespace as u8).context(EncodeLogKey)?;
        buf.try_put_u64(log_key.region_id).context(EncodeLogKey)?;
        buf.try_put_u64(log_key.table_id).context(EncodeLogKey)?;
        buf.try_put_u64(log_key.sequence_num).context(EncodeLogKey)?;
        buf.try_put_u8(self.version).context(EncodeLogKey)?;

        Ok(())
    }

    fn estimateEncodedSize(&self, _log_key: &CommonLogKey) -> usize {
        // Refer to key format.
        1 + 8 + 8 + 8 + 1
    }
}

impl Decoder<CommonLogKey> for CommonLogKeyEncoder {
    type Error = Error;

    fn decode<B: Buf>(&self, buf: &mut B) -> Result<CommonLogKey> {
        // Check namespace
        let namespace = buf.try_get_u8().context(DecodeLogKey)?;
        ensure!(
            namespace == self.namespace as u8,
            InvalidNamespace {
                expect: self.namespace,
                given: namespace
            }
        );

        let log_key = CommonLogKey {
            region_id: buf.try_get_u64().context(DecodeLogKey)?,
            table_id: buf.try_get_u64().context(DecodeLogKey)?,
            sequence_num: buf.try_get_u64().context(DecodeLogKey)?,
        };

        // Check version
        let version = buf.try_get_u8().context(DecodeLogKey)?;
        ensure!(
            version == self.version,
            InvalidVersion {
                expect: self.version,
                given: version
            }
        );

        Ok(log_key)
    }
}

#[derive(Debug, Clone)]
pub struct CommonLogEncoding {
    commonLogKeyEncoder: CommonLogKeyEncoder,
    logValEncoder: LogValueEncoder,
    // value decoder is created dynamically from the version,
    value_enc_version: u8,
}

impl CommonLogEncoding {
    pub fn newest() -> Self {
        Self {
            commonLogKeyEncoder: CommonLogKeyEncoder::newest(),
            logValEncoder: LogValueEncoder::newest(),
            value_enc_version: NEWEST_LOG_VALUE_ENCODING_VERSION,
        }
    }

    /// Encode [LogKey] into `buf` and caller should knows that the keys are
    /// ordered by ([RegionId], [SequenceNum]) so the caller can use this
    /// method to generate min/max key in specific scope(global or in some region).
    pub fn encode_key(&self, buf: &mut BytesMut, commonLogKey: &CommonLogKey) -> Result<()> {
        buf.clear();
        buf.reserve(self.commonLogKeyEncoder.estimateEncodedSize(commonLogKey));
        self.commonLogKeyEncoder.encode(buf, commonLogKey)?;
        Ok(())
    }

    pub fn encode_value(&self, buf: &mut BytesMut, payload: &impl Payload) -> Result<()> {
        buf.clear();
        buf.reserve(self.logValEncoder.estimateEncodedSize(payload));
        self.logValEncoder.encode(buf, payload)
    }

    pub fn is_log_key(&self, mut buf: &[u8]) -> Result<bool> {
        self.commonLogKeyEncoder.is_valid(&mut buf)
    }

    pub fn decode_key(&self, mut buf: &[u8]) -> Result<CommonLogKey> {
        self.commonLogKeyEncoder.decode(&mut buf)
    }

    pub fn decode_value<'a>(&self, buf: &'a [u8]) -> Result<&'a [u8]> {
        let value_dec = LogValueDecoder {
            version: self.value_enc_version,
        };

        value_dec.decode(buf)
    }
}