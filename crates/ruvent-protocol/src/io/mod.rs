//! Reading and writing frames on a byte stream.

use crate::error::ProtocolError;
use bytes::Bytes;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Reads one frame and returns the bytes after its length field. `None` means
/// the stream ended cleanly before a frame began.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R, max_frame: usize) -> io::Result<Option<Bytes>> {
    let mut length = [0u8; 4];
    let mut filled = 0;
    while filled < 4 {
        let read = reader.read(&mut length[filled..]).await?;
        if read == 0 {
            return if filled == 0 { Ok(None) } else { Err(io::ErrorKind::UnexpectedEof.into()) };
        }
        filled += read;
    }
    let length = u32::from_le_bytes(length) as usize;
    if length > max_frame {
        return Err(ProtocolError::FrameTooLarge(length).into());
    }
    let mut frame = vec![0u8; length];
    reader.read_exact(&mut frame).await?;
    Ok(Some(Bytes::from(frame)))
}

/// Writes whole frames (length field included) and flushes once.
pub async fn write_frames<W: AsyncWrite + Unpin>(writer: &mut W, frames: &[Bytes]) -> io::Result<()> {
    for frame in frames {
        writer.write_all(frame).await?;
    }
    writer.flush().await
}
