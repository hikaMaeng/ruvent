use super::*;
use crate::envelope::{EventId, WorkKey};

#[test]
fn a_snapshot_file_holds_both_kinds_and_reads_back() {
    let mut claimable = Envelope::claimable("orders", "create", Bytes::from_static(b"{}")).with_id(EventId::new_v7());
    claimable.key = Some(WorkKey { generation: 1, arrival: 2 });
    claimable.principal = Some("svc".into());
    let dispatch = Envelope::dispatch("orders", "created", Bytes::from_static(b"ok")).with_id(EventId::new_v7()).with_correlation(EventId::new_v7());
    let directory = std::env::temp_dir().join(format!("ruvent-snapshot-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("one.rvs");
    std::fs::write(&path, encode(&[claimable.clone(), dispatch.clone()]).unwrap()).unwrap();
    assert_eq!(read(&path).unwrap(), vec![claimable, dispatch]);
}

#[test]
fn the_configuration_picks_channels_and_types() {
    let mut config = SnapshotConfig::new("x", vec![Pattern("audit.*".into())]);
    assert!(config.wants("audit.login", "any"));
    assert!(!config.wants("orders", "any"));
    config.types = vec!["login".into()];
    assert!(config.wants("audit.x", "login") && !config.wants("audit.x", "logout"));
}
