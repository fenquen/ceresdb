// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Bytes format

use std::convert::TryFrom;

use bytes_ext::{Buf, BufMut, Bytes, BytesMut, SafeBuf, SafeBufMut};
use snafu::{ensure, ResultExt};

use crate::{
    compact::{
        DecodeEmptyValue, DecodeValue, DecodeVarint, EncodeValue, EncodeVarint, Error,
        MemCompactDecoder, MemCompactEncoder, Result, SkipDecodedValue, TryIntoUsize,
    },
    consts, varint, DecodeTo, Encoder,
};

impl Encoder<[u8]> for MemCompactEncoder {
    type Error = Error;

    // EncodeCompactBytes joins bytes with its length into a byte slice. It is more
    // efficient in both space and time compare to EncodeBytes. Note that the
    // encoded result is not memcomparable.
    fn encode<B: BufMut>(&self, buf: &mut B, value: &[u8]) -> Result<()> {
        varint::encode_varint(buf, value.len() as i64).context(EncodeVarint)?;
        buf.try_put(value).context(EncodeValue)?;
        Ok(())
    }

    fn estimate_encoded_size(&self, value: &[u8]) -> usize {
        consts::MAX_VARINT_BYTES + value.len()
    }
}

impl Encoder<Bytes> for MemCompactEncoder {
    type Error = Error;

    fn encode<B: BufMut>(&self, buf: &mut B, value: &Bytes) -> Result<()> {
        self.encode(buf, &value[..])
    }

    fn estimate_encoded_size(&self, value: &Bytes) -> usize {
        self.estimate_encoded_size(&value[..])
    }
}

impl DecodeTo<BytesMut> for MemCompactDecoder {
    type Error = Error;

    fn decode_to<B: Buf>(&self, buf: &mut B, value: &mut BytesMut) -> Result<()> {
        let v = usize::try_from(varint::decode_varint(buf).context(DecodeVarint)?)
            .context(TryIntoUsize)?;
        ensure!(buf.remaining() >= v, DecodeEmptyValue);
        value.try_put(&buf.chunk()[..v]).context(DecodeValue)?;
        buf.try_advance(v).context(SkipDecodedValue)?;
        Ok(())
    }
}