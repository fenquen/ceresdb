// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Varint for codec whose test is covered by compact/number.rs
use bytes_ext::{Buf, SafeBuf, SafeBufMut};
use macros::define_result;
use snafu::{Backtrace, ResultExt, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to encode varint, err:{}", source))]
    EncodeVarint { source: bytes_ext::Error },

    #[snafu(display("Insufficient bytes to decode value.\nBacktrace:\n{}", backtrace))]
    DecodeEmptyValue { backtrace: Backtrace },

    #[snafu(display("Insufficient bytes to decode value, err:{}", source))]
    DecodeValue { source: bytes_ext::Error },

    #[snafu(display("Value larger than 64 bits (overflow).\nBacktrace:\n{}", backtrace))]
    UvarintOverflow { backtrace: Backtrace },
}

define_result!(Error);

// from https://golang.org/src/encoding/binary/varint.go?s=2506:2545#L68
// PutVarint encodes an int64 into buf and returns the number of bytes written.
// If the buffer is too small, PutVarint will panic.
//
// ```go
// func PutVarint(buf []byte, x int64) int {
//      ux := uint64(x) << 1
//      if x < 0 {
//      ux = ^ux
//      }
//      return PutUvarint(buf, ux)
// }
// ```
pub fn encode_varint<B: SafeBufMut>(buf: &mut B, value: i64) -> Result<()> {
    let mut x = (value as u64) << 1;
    if value < 0 {
        x = !x;
    }
    encode_uvarint(buf, x)
}

//
// from https://golang.org/src/encoding/binary/varint.go?s=1611:1652#L31
//
// ```go
// func PutUvarint(buf []byte, x uint64) int {
// 	i := 0
// 	for x >= 0x80 {
// 		buf[i] = byte(x) | 0x80
// 		x >>= 7
// 		i++
// 	}
// 	buf[i] = byte(x)
// 	return i + 1
// }
// ```
pub fn encode_uvarint<B: SafeBufMut>(buf: &mut B, mut x: u64) -> Result<()> {
    while x >= 0x80 {
        buf.try_put_u8(x as u8 | 0x80).context(EncodeVarint)?;
        x >>= 7;
    }
    buf.try_put_u8(x as u8).context(EncodeVarint)?;
    Ok(())
}

// from https://golang.org/src/encoding/binary/varint.go?s=2955:2991#L84
// Varint decodes an int64 from buf and returns that value and the
// number of bytes read (> 0). If an error occurred, the value is 0
// and the number of bytes n is <= 0 with the following meaning:
//
// 	n == 0: buf too small
// 	n  < 0: value larger than 64 bits (overflow)
// 	        and -n is the number of bytes read
//
// ```go
// func Varint(buf []byte) (int64, int) {
//      ux, n := Uvarint(buf) // ok to continue in presence of error
//      x := int64(ux >> 1)
//      if ux&1 != 0 {
//          x = ^x
//      }
//      return x, n
//      }
//  ```
pub fn decode_varint<B: Buf>(buf: &mut B) -> Result<i64> {
    let ux = decode_uvarint(buf)?;
    let mut x = (ux >> 1) as i64;
    if ux & 1 != 0 {
        x = !x;
    }
    Ok(x)
}

// from https://golang.org/src/encoding/binary/varint.go?s=2070:2108#L50
// Uvarint decodes a uint64 from buf and returns that value and the
// number of bytes read (> 0). If an error occurred, the value is 0
// and the number of bytes n is <= 0 meaning:
//
// 	n == 0: buf too small
// 	n  < 0: value larger than 64 bits (overflow)
// 	        and -n is the number of bytes read
//
//  ```go
// func Uvarint(buf []byte) (uint64, int) {
//   var x uint64
//   var s uint
//   for i, b := range buf {
//    if b < 0x80 {
//    if i > 9 || i == 9 && b > 1 {
//      return 0, -(i + 1) // overflow
//    }
//    return x | uint64(b)<<s, i + 1
//    }
//     x |= uint64(b&0x7f) << s
//     s += 7
//   }
//    return 0, 0
//  }
//  ```
//
pub fn decode_uvarint<B: Buf>(buf: &mut B) -> Result<u64> {
    let mut x: u64 = 0;
    let mut s: usize = 0;
    let len = buf.remaining();
    for i in 0..len {
        let b = buf.try_get_u8().context(DecodeValue)?;
        if b < 0x80 {
            if i > 9 || i == 9 && b > 1 {
                return UvarintOverflow.fail(); // overflow
            }
            return Ok(x | u64::from(b) << s);
        }
        x |= u64::from(b & 0x7f) << s;
        s += 7;
    }
    DecodeEmptyValue.fail()
}