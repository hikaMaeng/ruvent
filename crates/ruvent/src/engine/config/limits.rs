//! Resource limits: the producer table, managed memory, storage.

use std::time::Duration;

/// B4, S15.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProducerConfig {
    /// How long an entry outlives its producer's last connection, unless the
    /// producer asked for longer in `Hello`.
    pub linger: Duration,
    /// The longest a producer may ask for.
    pub linger_max: Duration,
    /// Most entries. At the bound the oldest entry without a connection goes.
    pub max_entries: usize,
    /// Managed bytes counted per entry.
    pub entry_bytes: u64,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self { linger: Duration::from_secs(3600), linger_max: Duration::from_secs(7 * 24 * 3600), max_entries: 1_000_000, entry_bytes: 64 }
    }
}

/// S5, S22: managed memory and its watermark. Ratios are fractions.
#[derive(Clone, Debug, PartialEq)]
pub struct MemoryConfig {
    /// The managed memory limit in bytes. `None`: half the physical memory
    /// where it can be read (`/proc/meminfo`), else 8 GiB.
    pub limit: Option<u64>,
    /// Kept back from the budget for completions, checkpoints and relocation.
    pub reserve: f64,
    /// The high watermark, of the budget: cached payloads are released past it.
    pub high_watermark: f64,
    /// Cached payloads are released down to this fraction of the high watermark.
    pub release: f64,
    /// Managed bytes counted per event, besides its payload (metadata).
    pub slot_bytes: u64,
    /// S22: the queue depth the budget should hold. Reported in `Stats`;
    /// nothing is paused or refused when it is passed.
    pub depth_target: u64,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self { limit: None, reserve: 0.10, high_watermark: 0.80, release: 0.875, slot_bytes: 180, depth_target: 100_000_000 }
    }
}

/// Half the physical memory, read from `/proc/meminfo` where it exists.
fn physical_half() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = text.lines().find(|line| line.starts_with("MemTotal:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib.saturating_mul(1024) / 2)
}

impl MemoryConfig {
    /// The limit in force.
    pub fn resolved_limit(&self) -> u64 {
        self.limit.or_else(physical_half).unwrap_or(8 << 30)
    }
}

/// S3, S4, S6, S17: the segment container's values.
#[derive(Clone, Debug, PartialEq)]
pub struct StorageConfig {
    /// S3: a segment is rolled at this size.
    pub segment_bytes: u64,
    /// S3: relocation starts when dead-but-pinned bytes pass this fraction of the disk budget.
    pub relocate_at: f64,
    /// S4: slots per checkpoint slice.
    pub checkpoint_slice: usize,
    /// S4: a checkpoint starts when the tail passes this many times the last checkpoint (and one segment).
    pub checkpoint_tail_multiple: u64,
    /// S4: … or this many bytes (or the last checkpoint's size, if larger).
    pub checkpoint_tail_cap: u64,
    /// S6: segments kept back from the disk budget…
    pub disk_reserve_segments: u64,
    /// S6: … or this fraction of the capacity, whichever is larger.
    pub disk_reserve: f64,
    /// S6: the disk capacity the log may use, in bytes. The standard library
    /// cannot ask the file system, so the server says. `None`: no disk budget,
    /// no disk watermark and no relocation trigger; usage is still counted.
    pub disk_capacity: Option<u64>,
    /// S17: preallocate segments. Not implemented: the plan adds it only if
    /// M5 shows an effect.
    pub preallocate: bool,
    /// Cold payloads of a lane read ahead after its head went out cold (2.5).
    pub read_ahead: usize,
    /// Reads from segments at once; more wait in the engine (2.2).
    pub max_reads: usize,
    /// Checkpoint slices handed to the writer and not yet written (2.10, the
    /// walk's bound with `checkpoint_slice`).
    pub checkpoint_queue: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 64 << 20,
            relocate_at: 0.10,
            checkpoint_slice: 4096,
            checkpoint_tail_multiple: 8,
            checkpoint_tail_cap: 8 << 30,
            disk_reserve_segments: 4,
            disk_reserve: 0.10,
            disk_capacity: None,
            preallocate: false,
            read_ahead: 16,
            max_reads: 64,
            checkpoint_queue: 2,
        }
    }
}
