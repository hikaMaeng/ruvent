use super::*;
use crate::envelope::{Channel, EventId, WorkKey};

/// A test channel: a UUIDv7-shaped id derived from a label, the same every time.
pub fn ch(label: &str) -> Channel {
    let fnv = |seed: u64| label.bytes().fold(seed, |hash, byte| (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3));
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&fnv(0xcbf2_9ce4_8422_2325).to_le_bytes());
    bytes[8..].copy_from_slice(&fnv(0x9e37_79b9_7f4a_7c15).to_le_bytes());
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Channel(bytes)
}

#[test]
fn a_snapshot_file_holds_both_kinds_and_reads_back() {
    let mut claimable = Envelope::claimable(ch("orders"), 17, Bytes::from_static(b"{}")).with_id(EventId::new_v7());
    claimable.key = Some(WorkKey { generation: 1, arrival: 2 });
    let dispatch = Envelope::dispatch(ch("orders"), 2, Bytes::from_static(b"ok")).with_id(EventId::new_v7());
    let directory = std::env::temp_dir().join(format!("ruvent-snapshot-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("one.rvs");
    let bytes = encode(&[claimable.clone(), dispatch.clone()]).unwrap();
    assert!(bytes.starts_with(b"ruvsnap\x02"), "format 2: no name items");
    std::fs::write(&path, bytes).unwrap();
    assert_eq!(read(&path).unwrap(), vec![claimable, dispatch]);
}

#[test]
fn the_configuration_picks_types_on_every_channel() {
    let mut config = SnapshotConfig::new("x", vec![10, 19]);
    assert!(config.wants(10) && config.wants(19));
    assert!(!config.wants(11) && !config.wants(0));
    config.types = Vec::new();
    assert!(config.wants(10) && config.wants(u16::MAX), "no types listed collects every type");
}
