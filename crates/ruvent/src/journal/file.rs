use super::{Journal, Record, Recovered};
use async_trait::async_trait;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// `[len: u32][crc32: u32]` in front of every frame.
const HEADER: usize = 8;
/// `[kind: u8][prev: u64][through: u64]` at the start of every frame body.
const CHAIN: usize = 17;
const SNAPSHOT: u8 = 0;
const APPEND: u8 = 1;
const REWRITE_CHUNK: usize = 4096;

/// An append-only file of CRC-framed, chained batches.
///
/// ```text
/// [len u32 LE][crc32 u32 LE]  [kind u8][prev u64 LE][through u64 LE][bincode(Vec<Record>)]
///                             '------------------- covered by crc -------------------'
/// ```
///
/// Three independent things keep one write from damaging another, and none of
/// them is a clock:
///
/// * **a lock file**, held for the journal's lifetime, so a second process (or
///   a second engine in this one) cannot open the same journal at all;
/// * **the chain**: every appended frame names the record number it follows,
///   `append` refuses a batch that does not follow the last frame written, and
///   replay stops at the first frame that breaks the chain;
/// * **append-only writes**: nothing is updated in place, so a crash can only
///   tear the last frame, which then fails its length or CRC check and is cut
///   off on open. Compaction writes a new file and renames it over the old.
pub struct FileJournal {
    path: PathBuf,
    file: Option<File>,
    /// Record number the file ends at.
    chain: u64,
    _lock: File,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn frame(kind: u8, prev: u64, through: u64, records: &[Record]) -> io::Result<Vec<u8>> {
    let records = bincode::serialize(records).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let length = u32::try_from(CHAIN + records.len()).map_err(|_| invalid("journal batch exceeds 4 GiB"))?;
    let mut buffer = Vec::with_capacity(HEADER + CHAIN + records.len());
    buffer.extend_from_slice(&length.to_le_bytes());
    buffer.extend_from_slice(&[0; 4]);
    buffer.push(kind);
    buffer.extend_from_slice(&prev.to_le_bytes());
    buffer.extend_from_slice(&through.to_le_bytes());
    buffer.extend_from_slice(&records);
    let crc = crc32fast::hash(&buffer[HEADER..]);
    buffer[4..8].copy_from_slice(&crc.to_le_bytes());
    Ok(buffer)
}

fn open_append(path: &Path) -> io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

impl FileJournal {
    /// Opens (or creates) a journal and returns every record that survived.
    ///
    /// Fails if another engine already holds this journal.
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

        let mut records = Vec::new();
        let mut chain = 0u64;
        let mut good = 0usize;
        while bytes.len() - good >= HEADER {
            let length = u32::from_le_bytes(bytes[good..good + 4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(bytes[good + 4..good + 8].try_into().unwrap());
            let Some(body) = bytes.get(good + HEADER..good + HEADER + length) else { break };
            if length < CHAIN || crc32fast::hash(body) != crc {
                break;
            }
            let prev = u64::from_le_bytes(body[1..9].try_into().unwrap());
            let through = u64::from_le_bytes(body[9..17].try_into().unwrap());
            // A frame that does not follow its predecessor was written out of
            // turn. Nothing after it can be trusted.
            if body[0] == APPEND && prev != chain {
                break;
            }
            let Ok(batch) = bincode::deserialize::<Vec<Record>>(&body[CHAIN..]) else { break };
            records.extend(batch);
            chain = through;
            good += HEADER + length;
        }

        // Whatever follows the last intact frame is cut off, so the next
        // append continues the chain from a clean end.
        if good < bytes.len() {
            let file = OpenOptions::new().write(true).open(&path)?;
            file.set_len(good as u64)?;
            file.sync_all()?;
        }
        Ok((Self { file: Some(open_append(&path)?), path, chain, _lock: lock }, Recovered { records, through: chain }))
    }

    fn take(&mut self) -> io::Result<File> {
        self.file.take().ok_or_else(|| io::Error::other("journal is unusable after an earlier write failure"))
    }
}

#[async_trait]
impl Journal for FileJournal {
    async fn append(&mut self, prev: u64, through: u64, records: &[Record]) -> io::Result<()> {
        if prev != self.chain {
            return Err(invalid(format!("journal ends at record {} but the batch follows {prev}; refusing to write out of turn", self.chain)));
        }
        let buffer = frame(APPEND, prev, through, records)?;
        let mut file = self.take()?;
        // The handle is only put back on success: after a failed write or
        // fsync the file's contents are unknown, and continuing would stack
        // later batches on top of a hole.
        let file = tokio::task::spawn_blocking(move || -> io::Result<File> {
            file.write_all(&buffer)?;
            file.sync_data()?;
            Ok(file)
        })
        .await
        .map_err(io::Error::other)??;
        self.file = Some(file);
        self.chain = through;
        Ok(())
    }

    async fn rewrite(&mut self, at: u64, records: &[Record]) -> io::Result<()> {
        let mut buffer = Vec::new();
        for chunk in records.chunks(REWRITE_CHUNK) {
            buffer.extend_from_slice(&frame(SNAPSHOT, at, at, chunk)?);
        }
        if records.is_empty() {
            buffer.extend_from_slice(&frame(SNAPSHOT, at, at, &[])?);
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
            // Atomic: a crash leaves either the old journal or the new one.
            std::fs::rename(&temporary, &path)?;
            open_append(&path)
        })
        .await
        .map_err(io::Error::other)??;
        self.file = Some(file);
        self.chain = at;
        Ok(())
    }
}
