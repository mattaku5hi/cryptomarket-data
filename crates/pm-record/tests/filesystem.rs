#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use chrono::{TimeZone, Utc};
use pm_record::{Error, Limits, Writer, load};
use pm_schema::{ClockStatus, EventEnvelope, EventKind, sha256};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
static N: AtomicUsize = AtomicUsize::new(0);
fn root() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "pm-record-fs-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&p).unwrap();
    p
}
fn ev() -> EventEnvelope {
    let t = Utc.timestamp_opt(1_800_000_000, 0).single().unwrap();
    let p = vec![1];
    EventEnvelope {
        schema_version: 1,
        source: "s".into(),
        stream: "x".into(),
        collector_id: "c".into(),
        collector_region: "unknown".into(),
        connection_id: "x".into(),
        collector_session_id: "session".into(),
        ingest_sequence: 1,
        source_sequence: None,
        source_event_time: None,
        source_publish_time: None,
        received_at: t,
        available_at: t,
        monotonic_elapsed_nanos: 1,
        provider_time_unit: None,
        kind: EventKind::Gap,
        raw_payload: p.clone(),
        quality_flags: vec![],
        payload_sha256: sha256(&p),
        clock_status: ClockStatus::Unknown,
    }
}
fn committed(r: &PathBuf) {
    let mut w = Writer::create(r, "p", Limits::default()).unwrap();
    w.append(&ev()).unwrap();
    w.seal().unwrap();
}
#[cfg(unix)]
#[test]
fn symlink_partition_events_manifest_and_root_are_rejected() {
    let r = root();
    let outside = root();
    committed(&outside);
    let partition_link = r.join("p");
    std::os::unix::fs::symlink(outside.join("p"), &partition_link).unwrap();
    assert!(matches!(
        load(&r, "p", &Limits::default()),
        Err(Error::Corrupt(_))
    ));
    fs::remove_file(&partition_link).unwrap();
    committed(&r);
    fs::rename(r.join("p/events.jsonl"), r.join("events.saved")).unwrap();
    std::os::unix::fs::symlink(r.join("events.saved"), r.join("p/events.jsonl")).unwrap();
    assert!(matches!(
        load(&r, "p", &Limits::default()),
        Err(Error::Corrupt(_))
    ));
    fs::remove_file(r.join("p/events.jsonl")).unwrap();
    fs::rename(r.join("events.saved"), r.join("p/events.jsonl")).unwrap();
    fs::rename(r.join("p/manifest.json"), r.join("manifest.saved")).unwrap();
    std::os::unix::fs::symlink(r.join("manifest.saved"), r.join("p/manifest.json")).unwrap();
    assert!(matches!(
        load(&r, "p", &Limits::default()),
        Err(Error::Corrupt(_))
    ));
    fs::remove_file(r.join("p/manifest.json")).unwrap();
    fs::rename(r.join("manifest.saved"), r.join("p/manifest.json")).unwrap();
    let parent = r.parent().unwrap();
    let link = parent.join(format!("pm-record-root-link-{}", std::process::id()));
    std::os::unix::fs::symlink(&r, &link).unwrap();
    assert!(matches!(
        load(&link, "p", &Limits::default()),
        Err(Error::Invalid(_))
    ));
    fs::remove_file(link).unwrap();
    fs::remove_dir_all(outside).unwrap();
    fs::remove_dir_all(r).unwrap();
}
#[cfg(unix)]
#[test]
fn nonregular_event_manifest_and_partition_entries_rejected() {
    let r = root();
    committed(&r);
    fs::remove_file(r.join("p/manifest.json")).unwrap();
    fs::create_dir(r.join("p/manifest.json")).unwrap();
    assert!(matches!(
        load(&r, "p", &Limits::default()),
        Err(Error::Corrupt(_))
    ));
    fs::remove_dir_all(r.join("p")).unwrap();
    fs::write(r.join("p"), b"file").unwrap();
    assert!(matches!(
        load(&r, "p", &Limits::default()),
        Err(Error::Corrupt(_))
    ));
    fs::remove_dir_all(r).unwrap();
}
#[cfg(unix)]
#[test]
fn nonregular_event_entry_rejected() {
    let r = root();
    committed(&r);
    fs::remove_file(r.join("p/events.jsonl")).unwrap();
    fs::create_dir(r.join("p/events.jsonl")).unwrap();
    assert!(matches!(
        load(&r, "p", &Limits::default()),
        Err(Error::Corrupt(_))
    ));
    fs::remove_dir_all(r).unwrap();
}
