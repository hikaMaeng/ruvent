use super::codec::{decode_frames, encode_frame, FrameNames, APPEND, MAGIC, SNAPSHOT};
use super::{Journal, Record, Recovered};
use async_trait::async_trait;
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// Records per frame in a rewrite. A rewrite is one atomic rename, so the
/// split only bounds the size of one frame.
const REWRITE_CHUNK: usize = 4096;

/// An append-only file of CRC-framed, chained batch frames (format in `codec.rs`).
///
/// Three independent things keep one write from damaging another, and none of
/// them is a clock:
///
/// * **a lock file**, held for the journal's lifetime, so a second process (or
///   a second engine in this one) cannot open the same journal;
/// * **the chain**: every appended frame names the record number it follows,
///   and replay stops at the first frame that breaks it;
/// * **append-only writes**: a crash can only tear the last frame, which then
///   fails its length or CRC check and is cut off on open. A rewrite writes a
///   new file and renames it over the old one.
///
/// A file whose header is not this format's — a version 1 journal, or not a
/// journal at all — is refused with `InvalidData` and left byte for byte as it was.
pub struct FileJournal {
    path: PathBuf,
    file: Option<File>,
    chain: u64,
    names: FrameNames,
    _lock: File,
}

fn open_append(path: &Path) -> io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

impl FileJournal {
    /// Opens (or creates) a journal and returns every record that survived.
    pub fn open(path: impl Into<PathBuf>) -> io::Result<(Self, Recovered)> {
        let path = path.into();
        let lock = OpenOptions::new().create(true).write(true).truncate(false).open(path.with_extension("journal.lock"))?;
        lock.try_lock().map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, format!("journal {} is held by another engine", path.display())))?;

        let mut bytes = Vec::new();
        match File::open(&path) {
            Ok(mut file) => {
                file.read_to_end(&mut bytes)?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        // Nothing, or a header cut short while the journal was being created.
        if bytes.len() < MAGIC.len() && MAGIC.starts_with(&bytes) {
            let mut file = OpenOptions::new().create(true).write(true).truncate(true).open(&path)?;
            file.write_all(MAGIC)?;
            file.sync_all()?;
            bytes = MAGIC.to_vec();
        }
        let bytes = Bytes::from(bytes);
        let frames = decode_frames(&bytes).map_err(|error| io::Error::new(error.kind(), format!("{}: {error}", path.display())))?;

        // Whatever follows the last whole frame is cut off, so the next append
        // continues from a clean end.
        if frames.good < bytes.len() {
            let file = OpenOptions::new().write(true).open(&path)?;
            file.set_len(frames.good as u64)?;
            file.sync_all()?;
        }
        let journal = FileJournal { file: Some(open_append(&path)?), path, chain: frames.through, names: FrameNames::from_defined(&frames.names), _lock: lock };
        Ok((journal, Recovered { records: frames.records }))
    }

    fn take(&mut self) -> io::Result<File> {
        self.file.take().ok_or_else(|| io::Error::other("journal is unusable after an earlier write failure"))
    }
}

#[async_trait]
impl Journal for FileJournal {
    async fn append(&mut self, records: Vec<Record>) -> io::Result<()> {
        let (frame, through) = encode_frame(APPEND, self.chain, &records, &mut self.names)?;
        let mut file = self.take()?;
        // The handle is put back only on success: after a failed write or fsync
        // the file's contents are unknown.
        let file = tokio::task::spawn_blocking(move || -> io::Result<File> {
            file.write_all(&frame)?;
            file.sync_data()?;
            Ok(file)
        })
        .await
        .map_err(io::Error::other)??;
        self.file = Some(file);
        self.chain = through;
        Ok(())
    }

    async fn rewrite(&mut self, records: Vec<Record>) -> io::Result<()> {
        let mut names = FrameNames::default();
        let mut buffer = MAGIC.to_vec();
        let mut chain = 0;
        for chunk in records.chunks(REWRITE_CHUNK) {
            let (frame, through) = encode_frame(SNAPSHOT, chain, chunk, &mut names)?;
            buffer.extend_from_slice(&frame);
            chain = through;
        }
        // Closed before the rename: Windows will not replace a file that is open.
        drop(self.take()?);
        let path = self.path.clone();
        let file = tokio::task::spawn_blocking(move || -> io::Result<File> {
            let temporary = path.with_extension("journal.tmp");
            let mut file = File::create(&temporary)?;
            file.write_all(&buffer)?;
            file.sync_all()?;
            drop(file);
            std::fs::rename(&temporary, &path)?;
            open_append(&path)
        })
        .await
        .map_err(io::Error::other)??;
        self.file = Some(file);
        self.chain = chain;
        self.names = names;
        Ok(())
    }
}
