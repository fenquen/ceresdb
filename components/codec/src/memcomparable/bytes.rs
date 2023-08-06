// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Bytes format

use bytes_ext::{Buf, BufMut, Bytes, BytesMut, SafeBuf, SafeBufMut};
use snafu::{ensure, ResultExt};

use crate::{
    memcomparable::{
        DecodeValueGroup, DecodeValueMarker, DecodeValuePadding, EncodeValue, Error, MemComparable,
        Result, SkipPadding,
    },
    DecodeTo, Encoder,
};

const ENC_GROUP_SIZE: usize = 8;
const ENC_MARKER: u8 = 0xFF;
const ENC_PAD: u8 = 0x0;
const PADS: [u8; ENC_GROUP_SIZE] = [0; ENC_GROUP_SIZE];

impl Encoder<[u8]> for MemComparable {
    type Error = Error;

    // encode Bytes guarantees the encoded value is in ascending order for
    // comparison, encoding with the following rule:
    //  [group1][marker1]...[groupN][markerN]
    //  group is 8 bytes slice which is padding with 0.
    //  marker is `0xFF - padding 0 count`
    // For example:
    //
    // ```
    //   [] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
    //   [1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
    //   [1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
    //   [1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
    // ```
    //
    // Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
    fn encode<B: SafeBufMut>(&self, buf: &mut B, value: &[u8]) -> Result<()> {
        let value_len = value.len();
        for idx in (0..=value_len).step_by(ENC_GROUP_SIZE) {
            let remain = value_len - idx;
            let mut pad_count = 0;
            if remain >= ENC_GROUP_SIZE {
                buf.try_put(&value[idx..idx + ENC_GROUP_SIZE])
                    .context(EncodeValue)?;
            } else {
                pad_count = ENC_GROUP_SIZE - remain;
                buf.try_put(&value[idx..]).context(EncodeValue)?;
                buf.try_put(&PADS[..pad_count]).context(EncodeValue)?;
            }
            let marker = ENC_MARKER - pad_count as u8;
            buf.try_put_u8(marker).context(EncodeValue)?;
        }
        Ok(())
    }

    // Allocate more space to avoid unnecessary slice growing.
    // Assume that the byte slice size is about `(len(data) / encGroupSize + 1) *
    // (encGroupSize + 1)` bytes, that is `(len(data) / 8 + 1) * 9` in our
    // implement.
    fn estimate_encoded_size(&self, value: &[u8]) -> usize {
        (value.len() / ENC_GROUP_SIZE + 1) * (ENC_GROUP_SIZE + 1)
    }
}

impl Encoder<Bytes> for MemComparable {
    type Error = Error;

    fn encode<B: BufMut>(&self, buf: &mut B, value: &Bytes) -> Result<()> {
        self.encode(buf, &value[..])
    }

    fn estimate_encoded_size(&self, value: &Bytes) -> usize {
        self.estimate_encoded_size(&value[..])
    }
}

impl DecodeTo<BytesMut> for MemComparable {
    type Error = Error;

    // decode Bytes which is encoded by encode Bytes before,
    // returns the leftover bytes and decoded value if no error.
    fn decode_to<B: Buf>(&self, buf: &mut B, value: &mut BytesMut) -> Result<()> {
        loop {
            let b = buf.chunk();
            ensure!(b.len() > ENC_GROUP_SIZE, DecodeValueGroup);

            let group_bytes = &b[..ENC_GROUP_SIZE + 1];
            let group = &group_bytes[..ENC_GROUP_SIZE];
            let marker = group_bytes[ENC_GROUP_SIZE];
            let pad_count = usize::from(ENC_MARKER - marker);
            ensure!(
                pad_count <= ENC_GROUP_SIZE,
                DecodeValueMarker { group_bytes }
            );

            let real_group_size = ENC_GROUP_SIZE - pad_count;
            value
                .try_put(&group[..real_group_size])
                .context(EncodeValue)?;

            if pad_count != 0 {
                // Check validity of padding bytes.
                for v in &group[real_group_size..] {
                    ensure!(*v == ENC_PAD, DecodeValuePadding { group_bytes });
                }
                buf.try_advance(ENC_GROUP_SIZE + 1).context(SkipPadding)?;

                break;
            }
            buf.try_advance(ENC_GROUP_SIZE + 1).context(SkipPadding)?;
        }
        Ok(())
    }
}