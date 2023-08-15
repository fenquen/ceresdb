// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Provides utilities for byte arrays.
//!
//! Use Bytes instead of Vec<u8>. Currently just re-export bytes crate.

// Should not use bytes crate outside of this mod so we can replace the actual
// implementations if needed.
pub use bytes::{Buf, BufMut, Bytes, BytesMut};
use snafu::{ensure, Backtrace, Snafu};

/// Error of MemBuf/MemBufMut.
///
/// We do not use `std::io::Error` because it is too large.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to fill whole buffer.\nBacktrace:\n{}", backtrace))]
    UnexpectedEof { backtrace: Backtrace },

    #[snafu(display("Failed to write whole buffer.\nBacktrace:\n{}", backtrace))]
    WouldOverflow { backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Now is just an alias to `Vec<u8>`, prefer to use this alias instead of `Vec<u8>`
pub type ByteVec = Vec<u8>;

/// Read bytes from a buffer.
///
/// Unlike [`bytes::Buf`], failed read operations will throw error rather than
/// panic.
pub trait SafeBuf {
    /// Copy bytes from self into dst.
    ///
    /// The cursor is advanced by the number of bytes copied.
    ///
    /// Returns error if self does not have enough remaining bytes to fill dst.
    fn try_copy_to_slice(&mut self, dst: &mut [u8]) -> Result<()>;

    /// Advance the internal cursor of the Buf
    ///
    /// Returns error if the `cnt > self.remaining()`. Note the `remaining`
    /// method is provided by [`bytes::Buf`].
    fn try_advance(&mut self, cnt: usize) -> Result<()>;

    /// Gets an unsigned 8 bit integer from self and advance current position
    ///
    /// Returns error if the capacity is not enough
    fn try_get_u8(&mut self) -> Result<u8> {
        let mut buf = [0; 1];
        self.try_copy_to_slice(&mut buf)?;
        Ok(buf[0])
    }

    /// Gets an unsigned 32 bit integer from self in big-endian byte order and
    /// advance current position
    ///
    /// Returns error if the capacity is not enough
    fn try_get_u32(&mut self) -> Result<u32> {
        let mut buf = [0; 4];
        self.try_copy_to_slice(&mut buf)?;
        Ok(u32::from_be_bytes(buf))
    }

    /// Gets an unsigned 64 bit integer from self in big-endian byte order and
    /// advance current position
    ///
    /// Returns error if the capacity is not enough
    fn try_get_u64(&mut self) -> Result<u64> {
        let mut buf = [0; 8];
        self.try_copy_to_slice(&mut buf)?;
        Ok(u64::from_be_bytes(buf))
    }

    fn try_get_f64(&mut self) -> Result<f64> {
        let mut buf = [0; 8];
        self.try_copy_to_slice(&mut buf)?;
        Ok(f64::from_be_bytes(buf))
    }

    fn try_get_f32(&mut self) -> Result<f32> {
        let mut buf = [0; 4];
        self.try_copy_to_slice(&mut buf)?;
        Ok(f32::from_be_bytes(buf))
    }
}

/// Write bytes to a buffer.
///
/// Unlike [`bytes::BufMut`], failed write operations will throw error rather than panic.
pub trait SafeBufMut {
    /// Write bytes into self from src, advance the buffer position
    ///
    /// Returns error if the capacity is not enough
    fn try_put(&mut self, src: &[u8]) -> Result<()>;

    /// Write an unsigned 8 bit integer to self, advance the buffer position
    ///
    /// Returns error if the capacity is not enough
    fn try_put_u8(&mut self, n: u8) -> Result<()> {
        let src = [n];
        self.try_put(&src)
    }

    /// Writes an unsigned 32 bit integer to self in the big-endian byte order,
    /// advance the buffer position
    ///
    /// Returns error if the capacity is not enough
    fn try_put_u32(&mut self, n: u32) -> Result<()> {
        self.try_put(&n.to_be_bytes())
    }

    /// Writes an unsigned 64 bit integer to self in the big-endian byte order,
    /// advance the buffer position
    ///
    /// Returns error if the capacity is not enough
    fn try_put_u64(&mut self, n: u64) -> Result<()> {
        self.try_put(&n.to_be_bytes())
    }

    /// Writes an float 64 to self in the big-endian byte order,
    /// advance the buffer position
    ///
    /// Returns error if the capacity is not enough
    fn try_put_f64(&mut self, n: f64) -> Result<()> {
        self.try_put(&n.to_be_bytes())
    }

    /// Writes an float 32 to self in the big-endian byte order,
    /// advance the buffer position
    ///
    /// Returns error if the capacity is not enough
    fn try_put_f32(&mut self, n: f32) -> Result<()> {
        self.try_put(&n.to_be_bytes())
    }
}

impl<T> SafeBufMut for T where T: BufMut {
    fn try_put(&mut self, src: &[u8]) -> Result<()> {
        ensure!(self.remaining_mut() >= src.len(), WouldOverflow);
        self.put(src);

        Ok(())
    }
}

impl<T> SafeBuf for T where T: Buf {
    fn try_copy_to_slice(&mut self, dst: &mut [u8]) -> Result<()> {
        ensure!(self.remaining() >= dst.len(), UnexpectedEof);
        self.copy_to_slice(dst);

        Ok(())
    }

    fn try_advance(&mut self, cnt: usize) -> Result<()> {
        ensure!(self.remaining() >= cnt, UnexpectedEof);
        self.advance(cnt);

        Ok(())
    }
}