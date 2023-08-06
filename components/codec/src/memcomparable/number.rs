// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Number format

use bytes_ext::{SafeBuf, SafeBufMut};
use snafu::ResultExt;

use crate::{
    consts,
    memcomparable::{DecodeValue, EncodeValue, Error, MemComparable, Result},
    DecodeTo, Encoder,
};

impl Encoder<i64> for MemComparable {
    type Error = Error;

    fn encode<B: SafeBufMut>(&self, buf: &mut B, value: &i64) -> Result<()> {
        buf.try_put_u64(encode_int_to_cmp_uint(*value))
            .context(EncodeValue)?;
        Ok(())
    }

    fn estimate_encoded_size(&self, _value: &i64) -> usize {
        // flag + u64
        9
    }
}

impl DecodeTo<i64> for MemComparable {
    type Error = Error;

    fn decode_to<B: SafeBuf>(&self, buf: &mut B, value: &mut i64) -> Result<()> {
        *value = decode_cmp_uint_to_int(buf.try_get_u64().context(DecodeValue)?);
        Ok(())
    }
}

// encode_int_to_cmp_uint make int v to comparable uint type
fn encode_int_to_cmp_uint(v: i64) -> u64 {
    (v as u64) ^ consts::SIGN_MASK
}

// decode_cmp_uint_to_int decodes the u that encoded by encode_int_to_cmp_uint
fn decode_cmp_uint_to_int(u: u64) -> i64 {
    (u ^ consts::SIGN_MASK) as i64
}

impl Encoder<u64> for MemComparable {
    type Error = Error;

    fn encode<B: SafeBufMut>(&self, buf: &mut B, value: &u64) -> Result<()> {
        buf.try_put_u64(*value).context(EncodeValue)?;
        Ok(())
    }

    fn estimate_encoded_size(&self, _value: &u64) -> usize {
        // flag + u64
        9
    }
}

impl DecodeTo<u64> for MemComparable {
    type Error = Error;

    fn decode_to<B: SafeBuf>(&self, buf: &mut B, value: &mut u64) -> Result<()> {
        *value = buf.try_get_u64().context(DecodeValue)?;
        Ok(())
    }
}