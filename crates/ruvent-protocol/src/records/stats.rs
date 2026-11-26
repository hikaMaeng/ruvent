//! `StatsReply`'s head: every field of `ruvent::Stats`, in declaration order.
//!
//! ```text
//! waiting, delayed, claimed, oldest_ms, memory_used, memory_budget, memory_limit,
//! depth_capacity, depth_target, disk_used, disk_budget, disk_limit, segments, draining u8,
//! counters (15 varints, declaration order),
//! type count, (type u16, waiting, oldest_ms)…,
//! idle lane count, (channel 16, type u16, waiting)…,
//! root count, (root type u16, events, bytes, disk, paused u8)…
//! ```
//!
//! Each list is cut at [`LIST`] entries so the head stays within `u16`.

use super::fields::{boolean, put_bool, put_varint};
use crate::error::ProtocolError;
use bytes::BufMut;
use ruvent::envelope::codec::Reader;
use ruvent::{Channel, Counters, IdleLane, RootStat, Stats, TypeStat};

/// Most entries of each top-K list on the wire.
pub(crate) const LIST: usize = 256;

fn counters(counters: &Counters) -> [u64; 15] {
    [
        counters.retransmits,
        counters.duplicates,
        counters.stale,
        counters.expired,
        counters.isolated,
        counters.reclaimed,
        counters.faults,
        counters.over_limit,
        counters.pauses,
        counters.released,
        counters.read_ahead,
        counters.spilled_bytes,
        counters.relocated,
        counters.segments_removed,
        counters.checkpoints,
    ]
}

pub(crate) fn encode(stats: &Stats, out: &mut impl BufMut) {
    for value in [
        stats.waiting,
        stats.delayed,
        stats.claimed,
        stats.oldest_ms,
        stats.memory_used,
        stats.memory_budget,
        stats.memory_limit,
        stats.depth_capacity,
        stats.depth_target,
        stats.disk_used,
        stats.disk_budget,
        stats.disk_limit,
        stats.segments,
    ] {
        put_varint(out, value);
    }
    put_bool(out, stats.draining);
    for value in counters(&stats.counters) {
        put_varint(out, value);
    }
    let types = &stats.types[..stats.types.len().min(LIST)];
    put_varint(out, types.len() as u64);
    for entry in types {
        out.put_u16_le(entry.event_type);
        put_varint(out, entry.waiting);
        put_varint(out, entry.oldest_ms);
    }
    let lanes = &stats.idle_lanes[..stats.idle_lanes.len().min(LIST)];
    put_varint(out, lanes.len() as u64);
    for lane in lanes {
        out.put_slice(&lane.channel.0);
        out.put_u16_le(lane.event_type);
        put_varint(out, lane.waiting);
    }
    let roots = &stats.roots[..stats.roots.len().min(LIST)];
    put_varint(out, roots.len() as u64);
    for entry in roots {
        out.put_u16_le(entry.root);
        put_varint(out, entry.events);
        put_varint(out, entry.bytes);
        put_varint(out, entry.disk);
        put_bool(out, entry.paused);
    }
}

fn count(reader: &mut Reader<'_>) -> Result<usize, ProtocolError> {
    let count = reader.varint()?;
    if count > LIST as u64 {
        return Err(ProtocolError::TooLarge(count as usize));
    }
    Ok(count as usize)
}

pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Stats, ProtocolError> {
    let mut totals = [0u64; 13];
    for value in &mut totals {
        *value = reader.varint()?;
    }
    let draining = boolean(reader)?;
    let mut c = [0u64; 15];
    for value in &mut c {
        *value = reader.varint()?;
    }
    let counters = Counters {
        retransmits: c[0],
        duplicates: c[1],
        stale: c[2],
        expired: c[3],
        isolated: c[4],
        reclaimed: c[5],
        faults: c[6],
        over_limit: c[7],
        pauses: c[8],
        released: c[9],
        read_ahead: c[10],
        spilled_bytes: c[11],
        relocated: c[12],
        segments_removed: c[13],
        checkpoints: c[14],
    };
    let mut types = Vec::new();
    for _ in 0..count(reader)? {
        types.push(TypeStat { event_type: reader.u16()?, waiting: reader.varint()?, oldest_ms: reader.varint()? });
    }
    let mut idle_lanes = Vec::new();
    for _ in 0..count(reader)? {
        idle_lanes.push(IdleLane { channel: Channel(reader.bytes16()?), event_type: reader.u16()?, waiting: reader.varint()? });
    }
    let mut roots = Vec::new();
    for _ in 0..count(reader)? {
        roots.push(RootStat { root: reader.u16()?, events: reader.varint()?, bytes: reader.varint()?, disk: reader.varint()?, paused: boolean(reader)? });
    }
    let [waiting, delayed, claimed, oldest_ms, memory_used, memory_budget, memory_limit, depth_capacity, depth_target, disk_used, disk_budget, disk_limit, segments] = totals;
    Ok(Stats {
        waiting,
        delayed,
        claimed,
        oldest_ms,
        memory_used,
        memory_budget,
        memory_limit,
        depth_capacity,
        depth_target,
        disk_used,
        disk_budget,
        disk_limit,
        segments,
        draining,
        counters,
        types,
        idle_lanes,
        roots,
    })
}
