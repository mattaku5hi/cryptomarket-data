#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use chrono::TimeZone;
use pm_schema::{ClockStatus, EventKind, sha256};
use std::sync::atomic::{AtomicUsize, Ordering};
static ID: AtomicUsize = AtomicUsize::new(0);
fn root() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "pm-record-fault-{}-{}",
        std::process::id(),
        ID.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&p).unwrap();
    p
}
fn ev() -> EventEnvelope {
    let t = Utc.timestamp_opt(1_800_000_000, 0).single().unwrap();
    let payload = vec![1];
    EventEnvelope {
        schema_version: 1,
        source: "s".into(),
        stream: "x".into(),
        collector_id: "c".into(),
        collector_region: "unknown".into(),
        connection_id: "x".into(),
        collector_session_id: "sess".into(),
        ingest_sequence: 1,
        source_sequence: None,
        source_event_time: None,
        source_publish_time: None,
        received_at: t,
        available_at: t,
        monotonic_elapsed_nanos: 1,
        provider_time_unit: None,
        kind: EventKind::Gap,
        raw_payload: payload.clone(),
        quality_flags: vec![],
        payload_sha256: sha256(&payload),
        clock_status: ClockStatus::Unknown,
    }
}
#[test]
fn partition_classification_preserves_non_not_found_io() {
    let e = Error::Io(io::Error::new(io::ErrorKind::PermissionDenied, "test"));
    assert!(
        matches!(partition_dir_error(e),Error::Io(x) if x.kind()==io::ErrorKind::PermissionDenied)
    );
}
#[test]
fn all_injected_uncertain_io_paths_poison_and_preserve_artifacts() {
    for point in [
        FaultAt::PartialAppend,
        FaultAt::Flush,
        FaultAt::EventSync,
        FaultAt::ManifestWrite,
        FaultAt::ManifestSync,
        FaultAt::Publish,
        FaultAt::PartitionSync,
        FaultAt::RootSync,
    ] {
        let r = root();
        let mut w = Writer::create(&r, "p", Limits::default()).unwrap();
        if point == FaultAt::PartialAppend {
            w.fault = Some(point);
            assert!(matches!(w.append(&ev()), Err(Error::Io(_))));
        } else {
            w.append(&ev()).unwrap();
            w.fault = Some(point);
            assert!(matches!(w.seal(), Err(Error::Io(_))));
        }
        assert!(matches!(w.append(&ev()), Err(Error::Poisoned)));
        assert!(matches!(w.seal(), Err(Error::Poisoned)));
        assert!(r.join("p/events.jsonl").exists());
        if matches!(point, FaultAt::PartitionSync | FaultAt::RootSync) {
            assert!(r.join("p/manifest.json").exists());
            assert!(load(&r, "p", &Limits::default()).is_ok());
        }
        fs::remove_dir_all(r).unwrap();
    }
}

#[test]
fn durable_prefix_is_exact_append_only_and_not_a_committed_partition() {
    let r = root();
    let mut w = Writer::create(&r, "prefix", Limits::default()).unwrap();
    assert!(matches!(w.sync_prefix(), Err(Error::Invalid(_))));
    w.append(&ev()).unwrap();
    let first = w.sync_prefix().unwrap().manifest;
    let first_bytes = fs::read(r.join("prefix/events.jsonl")).unwrap();
    assert_eq!(first.event_bytes, u64::try_from(first_bytes.len()).unwrap());
    assert_eq!(first.sha256, sha256(&first_bytes));
    assert_eq!(first.record_count, 1);
    assert!(matches!(
        load(&r, "prefix", &Limits::default()),
        Err(Error::Uncommitted)
    ));
    assert!(!r.join("prefix/manifest.json").exists());
    let mut second = ev();
    second.ingest_sequence = 2;
    second.monotonic_elapsed_nanos = 2;
    w.append(&second).unwrap();
    let next = w.sync_prefix().unwrap().manifest;
    let all_bytes = fs::read(r.join("prefix/events.jsonl")).unwrap();
    assert!(all_bytes.starts_with(&first_bytes));
    assert_eq!(next.record_count, 2);
    assert_eq!(next.event_bytes, u64::try_from(all_bytes.len()).unwrap());
    assert_eq!(next.sha256, sha256(&all_bytes));
    assert_eq!(w.seal().unwrap().manifest, next);
    assert_eq!(load(&r, "prefix", &Limits::default()).unwrap().0, next);
    assert!(matches!(w.sync_prefix(), Err(Error::Sealed)));
    fs::remove_dir_all(r).unwrap();
}

#[test]
fn each_prefix_sync_failure_poison_latches_without_publishing_a_manifest() {
    for point in [
        FaultAt::Flush,
        FaultAt::EventSync,
        FaultAt::PartitionSync,
        FaultAt::RootSync,
    ] {
        let r = root();
        let mut w = Writer::create(&r, "prefix", Limits::default()).unwrap();
        w.append(&ev()).unwrap();
        w.fault = Some(point);
        assert!(matches!(w.sync_prefix(), Err(Error::Io(_))), "{point:?}");
        assert!(matches!(w.sync_prefix(), Err(Error::Poisoned)));
        assert!(matches!(w.append(&ev()), Err(Error::Poisoned)));
        assert!(matches!(w.seal(), Err(Error::Poisoned)));
        assert!(r.join("prefix/events.jsonl").exists());
        assert!(!r.join("prefix/manifest.json").exists());
        fs::remove_dir_all(r).unwrap();
    }
}

#[test]
fn partial_append_cannot_be_acknowledged_as_a_durable_prefix() {
    let r = root();
    let mut w = Writer::create(&r, "prefix", Limits::default()).unwrap();
    w.fault = Some(FaultAt::PartialAppend);
    assert!(matches!(w.append(&ev()), Err(Error::Io(_))));
    assert!(matches!(w.sync_prefix(), Err(Error::Poisoned)));
    assert_eq!(fs::read(r.join("prefix/events.jsonl")).unwrap().len(), 3);
    fs::remove_dir_all(r).unwrap();
}
