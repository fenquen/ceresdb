// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::mem;

use bytes_ext::{SafeBuf, SafeBufMut};
use snafu::ResultExt;

use crate::{
    compact::{DecodeValue, EncodeValue, Error, MemCompactDecoder, MemCompactEncoder, Result},
    DecodeTo, Encoder,
};

impl Encoder<f64> for MemCompactEncoder {
    type Error = Error;

    fn encode<B: SafeBufMut>(&self, buf: &mut B, value: &f64) -> Result<()> {
        buf.try_put_f64(*value).context(EncodeValue)?;
        Ok(())
    }

    fn estimate_encoded_size(&self, _value: &f64) -> usize {
        mem::size_of::<f64>()
    }
}

impl DecodeTo<f64> for MemCompactDecoder {
    type Error = Error;

    fn decode_to<B: SafeBuf>(&self, buf: &mut B, value: &mut f64) -> Result<()> {
        *value = buf.try_get_f64().context(DecodeValue)?;
        Ok(())
    }
}

impl Encoder<f32> for MemCompactEncoder {
    type Error = Error;

    fn encode<B: SafeBufMut>(&self, buf: &mut B, value: &f32) -> Result<()> {
        buf.try_put_f32(*value).context(EncodeValue)?;
        Ok(())
    }

    fn estimate_encoded_size(&self, _value: &f32) -> usize {
        mem::size_of::<f32>()
    }
}

impl DecodeTo<f32> for MemCompactDecoder {
    type Error = Error;

    fn decode_to<B: SafeBuf>(&self, buf: &mut B, value: &mut f32) -> Result<()> {
        *value = buf.try_get_f32().context(DecodeValue)?;
        Ok(())
    }
}
