//! Reading records head first and writing them with vectored writes.
//!
//! * [`RecordReader`] reads a record's header and head, so the caller learns
//!   the body's length — and, through the decoder, where the body belongs —
//!   before reading it. A body is read into its own buffer (straight from the
//!   socket past the direct threshold, S11), never into a buffer shared with
//!   other records, so a small long-lived payload never holds a large read
//!   buffer. Nothing allocates a whole frame up front: there is no frame.
//! * [`Parts`] is what an encoder gathered: written with `write_vectored`,
//!   continuing after a partial write from where it stopped, and counting
//!   what reached the socket.

use crate::error::ProtocolError;
use crate::{DIRECT, MAX_BODY};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::VecDeque;
use std::io::{self, IoSlice};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// `[tag u8][head_len u16][body_len u32]`.
pub const HEADER: usize = 7;

/// Bytes asked of the socket per read while reading heads and small bodies.
const READ: usize = 64 << 10;

/// Vector entries handed to one `write_vectored` call.
const VECTOR: usize = 64;

/// A record whose head is read and whose body is not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawRecord {
    pub tag: u8,
    pub head: Bytes,
    pub body_len: u32,
}

/// Reads records from a byte stream.
///
/// The read buffer:
///
/// | key | the stream |
/// | --- | --- |
/// | scope | one direction of one connection |
/// | lifetime | the connection |
/// | bound | one read ([`READ`]) past the longest head (`u16`); bodies are not kept in it |
/// | cleared by | consumption, record by record |
pub struct RecordReader<R> {
    inner: R,
    buf: BytesMut,
    direct: usize,
    max_body: usize,
}

impl<R: AsyncRead + Unpin> RecordReader<R> {
    pub fn new(inner: R) -> Self {
        Self::with_limits(inner, DIRECT, MAX_BODY)
    }

    /// `direct`: a body of at least this many bytes is read straight into its
    /// own buffer (S11). `max_body`: the largest body a record may carry (B13).
    pub fn with_limits(inner: R, direct: usize, max_body: usize) -> Self {
        RecordReader { inner, buf: BytesMut::with_capacity(READ), direct: direct.max(1), max_body }
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    /// Makes at least `need` bytes buffered. `Ok(false)`: the stream ended
    /// cleanly before the first of them, and `clean` allowed that.
    async fn fill(&mut self, need: usize, clean: bool) -> io::Result<bool> {
        while self.buf.len() < need {
            self.buf.reserve((need - self.buf.len()).max(READ));
            if self.inner.read_buf(&mut self.buf).await? == 0 {
                if clean && self.buf.is_empty() {
                    return Ok(false);
                }
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
        }
        Ok(true)
    }

    /// Up to `count` buffered bytes, reading once more only when fewer are
    /// there; fewer when the stream ends first.
    pub async fn peek(&mut self, count: usize) -> io::Result<&[u8]> {
        while self.buf.len() < count {
            self.buf.reserve(READ);
            if self.inner.read_buf(&mut self.buf).await? == 0 {
                break;
            }
        }
        Ok(&self.buf[..self.buf.len().min(count)])
    }

    /// The next record's header and head. `None`: the stream ended cleanly
    /// at a record boundary.
    pub async fn head(&mut self) -> io::Result<Option<RawRecord>> {
        if !self.fill(HEADER, true).await? {
            return Ok(None);
        }
        let tag = self.buf[0];
        let head_len = usize::from(u16::from_le_bytes([self.buf[1], self.buf[2]]));
        let body_len = u32::from_le_bytes([self.buf[3], self.buf[4], self.buf[5], self.buf[6]]);
        if body_len as usize > self.max_body {
            return Err(ProtocolError::TooLarge(body_len as usize).into());
        }
        self.fill(HEADER + head_len, false).await?;
        self.buf.advance(HEADER);
        let head = self.buf.split_to(head_len).freeze();
        Ok(Some(RawRecord { tag, head, body_len }))
    }

    /// The body of the record whose head was just read, in its own buffer.
    pub async fn body(&mut self, len: u32) -> io::Result<Bytes> {
        let len = len as usize;
        if len == 0 {
            return Ok(Bytes::new());
        }
        if len < self.direct {
            self.fill(len, false).await?;
            let body = Bytes::copy_from_slice(&self.buf[..len]);
            self.buf.advance(len);
            return Ok(body);
        }
        let mut body = BytesMut::with_capacity(len);
        let have = self.buf.len().min(len);
        body.put_slice(&self.buf[..have]);
        self.buf.advance(have);
        while body.len() < len {
            let left = len - body.len();
            if self.inner.read_buf(&mut (&mut body).limit(left)).await? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
        }
        Ok(body.freeze())
    }

    /// The next piece of a body being streamed elsewhere (to disk): at most
    /// `max` bytes, at least one. The caller counts what is left.
    pub async fn chunk(&mut self, max: usize) -> io::Result<Bytes> {
        if self.buf.is_empty() {
            self.fill(1, false).await?;
        }
        let take = self.buf.len().min(max.max(1));
        Ok(self.buf.split_to(take).freeze())
    }

    /// Reads and drops everything until the stream ends: the last step of a
    /// staged close, so the peer's unread bytes never turn the close into a
    /// reset that would erase the `Bye` it has not read yet (2.2).
    pub async fn discard(&mut self) -> io::Result<()> {
        self.buf.clear();
        loop {
            self.buf.reserve(READ);
            if self.inner.read_buf(&mut self.buf).await? == 0 {
                return Ok(());
            }
            self.buf.clear();
        }
    }
}

/// What an encoder gathered, written with vectored writes.
#[derive(Debug, Default)]
pub struct Parts {
    parts: VecDeque<Bytes>,
    total: u64,
    written: u64,
}

impl Parts {
    pub fn new(parts: Vec<Bytes>) -> Self {
        let total = parts.iter().map(|part| part.len() as u64).sum();
        Parts { parts: parts.into(), total, written: 0 }
    }

    /// Bytes in all, written or not.
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Bytes that reached the writer so far.
    pub fn written(&self) -> u64 {
        self.written
    }

    pub fn is_empty(&self) -> bool {
        self.written == self.total
    }

    /// The bytes not yet written, in one buffer (a copy: for tests and diagnostics).
    pub fn concat(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity((self.total - self.written) as usize);
        for part in &self.parts {
            out.extend_from_slice(part);
        }
        out
    }

    /// Appends another batch after this one.
    pub fn extend(&mut self, other: Parts) {
        self.total += other.total - other.written;
        self.parts.extend(other.parts);
    }

    fn advance(&mut self, mut count: usize) {
        self.written += count as u64;
        while count > 0 {
            let Some(front) = self.parts.front_mut() else { return };
            if count >= front.len() {
                count -= front.len();
                self.parts.pop_front();
            } else {
                front.advance(count);
                count = 0;
            }
        }
    }

    /// Writes everything, continuing after partial writes, then flushes.
    /// On an error, [`Parts::written`] says how far it got.
    pub async fn write_to<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> io::Result<()> {
        while !self.parts.is_empty() {
            let wrote = {
                let slices: Vec<IoSlice<'_>> = self.parts.iter().take(VECTOR).map(|part| IoSlice::new(part)).collect();
                writer.write_vectored(&slices).await?
            };
            if wrote == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            self.advance(wrote);
        }
        writer.flush().await
    }
}

/// Whether `first` (a connection's first bytes) is a version-1 hello:
/// `[length u32][count u32 = 1][tag 0x01][version u16 = 1]`. A version-2
/// stream never starts like that: its first byte is the `Hello` tag, and a
/// version-1 hello frame is never one byte long.
pub fn sniff_v1(first: &[u8]) -> bool {
    first.len() >= 11 && first[4..8] == [1, 0, 0, 0] && first[8] == 0x01 && first[9..11] == [1, 0]
}

/// A version-1 `Bye` frame with `reason`, so a version-1 client can say why
/// it was refused: `[length u32][count u32 = 1][tag 0x03][length u16][text]`.
pub fn v1_bye(reason: &str) -> Vec<u8> {
    let text = &reason.as_bytes()[..reason.len().min(u16::MAX as usize - 16)];
    let mut frame = Vec::with_capacity(15 + text.len());
    frame.put_u32_le((4 + 1 + 2 + text.len()) as u32);
    frame.put_u32_le(1);
    frame.put_u8(0x03);
    frame.put_u16_le(text.len() as u16);
    frame.put_slice(text);
    frame
}
