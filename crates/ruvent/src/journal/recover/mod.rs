//! Streaming recovery (2.5): the checkpoint, then the log from the
//! checkpoint's start, read as recovery consumes it.
//!
//! Nothing is read whole. Payloads are hashed for the frame check and skipped;
//! a `Register` comes out with its payload's location instead. So recovery
//! holds the metadata it rebuilds and one frame's metadata, never the log.
//!
//! The tail is replayed from the last log segment whose first serial is at or
//! before the checkpoint's start; records before the start are skipped. The
//! serial chain must hold from there to the end: a frame that does not start
//! where the previous one ended, or a damaged frame anywhere, stops recovery
//! with an error. The torn tail of the last segment was cut at open.

use super::checkpoint::CheckpointReader;
use super::codec::{invalid, read_frame, Defined, Frame, Stream, SEGMENT_HEADER};
use super::segment::{segment_path, Found};
use super::{End, Replay, Source};
use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, BufReader, Seek, SeekFrom};
use std::path::PathBuf;

/// The segment being read.
struct Reading {
    index: usize,
    stream: Stream<BufReader<File>>,
    defined: Defined,
    len: u64,
}

enum Phase {
    Begin,
    Checkpoint(CheckpointReader),
    Log(Option<Reading>, usize),
    Done,
}

/// The source a [`super::SegmentLog`] hands out at open.
pub(in crate::journal) struct LogSource {
    dir: PathBuf,
    log: Vec<Found>,
    checkpoint: Option<PathBuf>,
    phase: Phase,
    buffered: VecDeque<Replay>,
    start: u64,
    expected: Option<u64>,
    end: End,
}

impl LogSource {
    pub fn new(dir: PathBuf, log: Vec<Found>, checkpoint: Option<PathBuf>) -> Self {
        LogSource { dir, log, checkpoint, phase: Phase::Begin, buffered: VecDeque::new(), start: 0, expected: None, end: End::default() }
    }

    /// The segment replay starts in: the last one whose first serial is at or
    /// before the start.
    fn first_segment(&self) -> usize {
        self.log.iter().rposition(|segment| segment.first <= self.start).unwrap_or(0)
    }

    fn open(&self, index: usize) -> io::Result<Reading> {
        let found = self.log[index];
        let mut file = File::open(segment_path(&self.dir, found.number))?;
        file.seek(SeekFrom::Start(SEGMENT_HEADER))?;
        Ok(Reading { index, stream: Stream::new(BufReader::new(file), SEGMENT_HEADER), defined: Defined::default(), len: found.bytes })
    }

    fn step(&mut self) -> io::Result<()> {
        match std::mem::replace(&mut self.phase, Phase::Done) {
            Phase::Begin => {
                self.phase = match self.checkpoint.take() {
                    Some(path) => Phase::Checkpoint(CheckpointReader::open(path)?),
                    None => Phase::Log(None, self.first_segment()),
                };
            }
            Phase::Checkpoint(mut reader) => match reader.next()? {
                Some(item) => {
                    if let Replay::Head(head) = &item {
                        self.start = head.start;
                        self.end = End { uptime: head.uptime, serial: head.start, commit: head.commit };
                    }
                    self.buffered.push_back(item);
                    self.phase = Phase::Checkpoint(reader);
                }
                None => self.phase = Phase::Log(None, self.first_segment()),
            },
            Phase::Log(None, index) => {
                if index < self.log.len() {
                    self.phase = Phase::Log(Some(self.open(index)?), index);
                }
            }
            Phase::Log(Some(mut reading), index) => {
                let number = self.log[reading.index].number;
                match read_frame(&mut reading.stream, reading.len, number, &mut reading.defined)? {
                    Frame::Whole { prefix, items } => {
                        match self.expected {
                            Some(expected) if prefix.first != expected => {
                                return Err(invalid(format!(
                                    "segment {number}: the serial chain breaks (a frame starts at {} where {expected} was next); the server stops rather than skip records",
                                    prefix.first
                                )));
                            }
                            None if prefix.first > self.start => {
                                return Err(invalid(format!("segment {number}: records {} to {} are missing; the server stops rather than skip them", self.start, prefix.first)));
                            }
                            _ => {}
                        }
                        self.expected = Some(prefix.next);
                        self.end.uptime = self.end.uptime.max(prefix.uptime);
                        self.end.commit = self.end.commit.max(prefix.commit);
                        self.end.serial = self.end.serial.max(prefix.next);
                        let start = self.start;
                        self.buffered.extend(items.into_iter().filter(|item| !matches!(item, Replay::Record { serial, .. } if *serial < start)));
                        self.phase = Phase::Log(Some(reading), index);
                    }
                    Frame::End => self.phase = Phase::Log(None, index + 1),
                    Frame::Torn { at } if index + 1 == self.log.len() => {
                        return Err(invalid(format!("segment {number}: a torn frame at byte {at} that the open did not cut")));
                    }
                    Frame::Torn { at } => return Err(invalid(format!("segment {number}: a frame cut short at byte {at} in the middle of the log; the server stops rather than skip what follows"))),
                    Frame::Damaged { at, why } => return Err(invalid(format!("segment {number}: damaged at byte {at}: {why}; the server stops rather than skip what follows"))),
                }
            }
            Phase::Done => {}
        }
        Ok(())
    }
}

impl Source for LogSource {
    fn next(&mut self) -> io::Result<Option<Replay>> {
        loop {
            if let Some(item) = self.buffered.pop_front() {
                return Ok(Some(item));
            }
            if matches!(self.phase, Phase::Done) {
                return Ok(None);
            }
            self.step()?;
        }
    }

    fn end(&self) -> End {
        self.end
    }
}
