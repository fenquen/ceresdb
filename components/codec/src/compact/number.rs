// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Number format

use bytes_ext::{Buf, SafeBufMut};
use snafu::ResultExt;

use crate::{
    compact::{DecodeVarint, EncodeVarint, Error, MemCompactDecoder, MemCompactEncoder, Result},
    consts, varint, DecodeTo, Encoder,
};

impl Encoder<i64> for MemCompactEncoder {
    type Error = Error;

    fn encode<B: SafeBufMut>(&self, buf: &mut B, value: &i64) -> Result<()> {
        varint::encode_varint(buf, *value).context(EncodeVarint)?;
        Ok(())
    }

    fn estimate_encoded_size(&self, _value: &i64) -> usize {
        consts::MAX_VARINT_BYTES
    }
}

impl DecodeTo<i64> for MemCompactDecoder {
    type Error = Error;

    fn decode_to<B: Buf>(&self, buf: &mut B, value: &mut i64) -> Result<()> {
        *value = varint::decode_varint(buf).context(DecodeVarint)?;
        Ok(())
    }
}

impl Encoder<u64> for MemCompactEncoder {
    type Error = Error;

    fn encode<B: SafeBufMut>(&self, buf: &mut B, value: &u64) -> Result<()> {
        varint::encode_uvarint(buf, *value).context(EncodeVarint)?;
        Ok(())
    }

    fn estimate_encoded_size(&self, _value: &u64) -> usize {
        consts::MAX_UVARINT_BYTES
    }
}

impl DecodeTo<u64> for MemCompactDecoder {
    type Error = Error;

    fn decode_to<B: Buf>(&self, buf: &mut B, value: &mut u64) -> Result<()> {
        *value = varint::decode_uvarint(buf).context(DecodeVarint)?;
        Ok(())
    }
}