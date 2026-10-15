#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::as_conversions,
    clippy::field_reassign_with_default
)]
use chrono::{TimeZone, Utc};
use pm_record::{Error, Inspection, Limits, Writer, inspect, load};
use pm_schema::{ClockStatus, EventEnvelope, EventKind, sha256};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
static NEXT: AtomicUsize = AtomicUsize::new(0);
fn root() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "pm-record-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&p).expect("temp");
    p
}
fn event(seq: u64, payload: Vec<u8>) -> EventEnvelope {
    let t = Utc
        .timestamp_opt(1_800_000_000, 0)
        .single()
        .expect("synthetic timestamp");
    EventEnvelope {
        schema_version: 1,
        source: "synthetic".into(),
        stream: "stream".into(),
        collector_id: "c".into(),
        collector_region: "unknown".into(),
        connection_id: "conn".into(),
        collector_session_id: "s".into(),
        ingest_sequence: seq,
        source_sequence: None,
        source_event_time: None,
        source_publish_time: None,
        received_at: t,
        available_at: t,
        monotonic_elapsed_nanos: seq,
        provider_time_unit: None,
        kind: EventKind::Gap,
        raw_payload: payload.clone(),
        quality_flags: vec!["SYNTHETIC_FIXTURE".into()],
        payload_sha256: sha256(&payload),
        clock_status: ClockStatus::Unknown,
    }
}
fn append_seal(r: &PathBuf, id: &str, events: &[EventEnvelope], limits: Limits) {
    let mut w = Writer::create(r, id, limits).expect("create");
    for e in events {
        w.append(e).expect("stage")
    }
    w.seal().expect("seal");
}
#[test]
fn binary_bytes_survive_seal_drop_reopen() {
    let r = root();
    append_seal(
        &r,
        "bin",
        &[event(9, vec![0, 255, 1, 128])],
        Limits::default(),
    );
    let (_, v) = load(&r, "bin", &Limits::default()).expect("load");
    assert_eq!(v[0].raw_payload, [0, 255, 1, 128]);
    fs::remove_dir_all(r).expect("cleanup")
}
#[test]
fn equal_input_produces_deterministic_event_and_manifest_bytes() {
    let a = root();
    let b = root();
    let e = event(1, vec![3, 0, 250]);
    append_seal(&a, "same", std::slice::from_ref(&e), Limits::default());
    append_seal(&b, "same", std::slice::from_ref(&e), Limits::default());
    assert_eq!(
        fs::read(a.join("same/events.jsonl")).unwrap(),
        fs::read(b.join("same/events.jsonl")).unwrap()
    );
    let manifest_a = fs::read(a.join("same/manifest.json")).unwrap();
    let manifest_b = fs::read(b.join("same/manifest.json")).unwrap();
    assert_eq!(manifest_a, manifest_b);
    let events = fs::read(a.join("same/events.jsonl")).unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&manifest_a).unwrap();
    assert_eq!(parsed["sha256"], sha256(&events));
    fs::remove_dir_all(a).unwrap();
    fs::remove_dir_all(b).unwrap()
}
#[test]
fn unknown_clock_gap_quality_and_late_source_time_preserve_ingest_order() {
    let r = root();
    let mut a = event(1, vec![1]);
    a.source_event_time = Some(Utc.timestamp_opt(10, 0).single().unwrap());
    a.provider_time_unit = Some("ns".into());
    let mut b = event(2, vec![2]);
    b.source_event_time = Some(Utc.timestamp_opt(1, 0).single().unwrap());
    b.provider_time_unit = Some("ns".into());
    append_seal(&r, "late", &[a, b], Limits::default());
    let (_, got) = load(&r, "late", &Limits::default()).unwrap();
    assert_eq!(got[0].ingest_sequence, 1);
    assert_eq!(got[1].ingest_sequence, 2);
    assert_eq!(got[0].kind, EventKind::Gap);
    assert_eq!(got[0].clock_status, ClockStatus::Unknown);
    assert_eq!(got[0].quality_flags, vec!["SYNTHETIC_FIXTURE"]);
    assert_eq!(
        got[0].source_event_time.unwrap(),
        Utc.timestamp_opt(10, 0).single().unwrap()
    );
    assert_eq!(
        got[1].source_event_time.unwrap(),
        Utc.timestamp_opt(1, 0).single().unwrap()
    );
    fs::remove_dir_all(r).unwrap()
}
#[test]
fn multiple_source_streams_same_session_valid() {
    let r = root();
    let a = event(4, vec![1]);
    let mut b = event(5, vec![2]);
    b.source = "other".into();
    b.stream = "second".into();
    b.connection_id = "new".into();
    append_seal(&r, "multi", &[a.clone(), b], Limits::default());
    assert_eq!(load(&r, "multi", &Limits::default()).unwrap().1.len(), 2);
    fs::remove_dir_all(r).unwrap();
}
#[test]
fn invalid_ids_refused_for_create_and_read_path() {
    let r = root();
    for id in ["", "../x", "/tmp", "..", "a/b"] {
        assert!(matches!(
            Writer::create(&r, id, Limits::default()),
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            load(&r, id, &Limits::default()),
            Err(Error::Invalid(_))
        ));
    }
    assert!(!r.parent().unwrap().join("x").exists());
    fs::remove_dir_all(r).unwrap()
}
#[test]
fn zero_limits_rejected() {
    for i in 0..5 {
        let mut l = Limits::default();
        match i {
            0 => l.max_payload_bytes = 0,
            1 => l.max_record_bytes = 0,
            2 => l.max_partition_bytes = 0,
            3 => l.max_records = 0,
            _ => l.max_manifest_bytes = 0,
        };
        let r = root();
        assert!(matches!(
            Writer::create(&r, "zero", l.clone()),
            Err(Error::Invalid(_))
        ));
        assert!(matches!(load(&r, "missing", &l), Err(Error::Invalid(_))));
        fs::remove_dir_all(r).unwrap()
    }
}
#[test]
fn record_limit_includes_newline_and_boundary_roundtrips() {
    let r = root();
    let e = event(1, vec![1]);
    let mut w = Writer::create(&r, "measure", Limits::default()).unwrap();
    w.append(&e).unwrap();
    let needed = fs::metadata(r.join("measure/events.jsonl")).unwrap().len();
    drop(w);
    fs::remove_dir_all(r).unwrap();
    let r = root();
    let mut limits = Limits::default();
    limits.max_record_bytes = needed;
    limits.max_partition_bytes = needed;
    append_seal(&r, "boundary", std::slice::from_ref(&e), limits.clone());
    assert!(load(&r, "boundary", &limits).is_ok());
    let mut payload_exact = Limits::default();
    payload_exact.max_payload_bytes = 1;
    append_seal(
        &r,
        "payload-exact",
        &[event(1, vec![1])],
        payload_exact.clone(),
    );
    assert!(load(&r, "payload-exact", &payload_exact).is_ok());
    append_seal(
        &r,
        "manifest-exact",
        &[event(1, vec![1])],
        Limits::default(),
    );
    let manifest_len = fs::metadata(r.join("manifest-exact/manifest.json"))
        .unwrap()
        .len();
    fs::remove_dir_all(r.join("manifest-exact")).unwrap();
    let mut manifest_exact = Limits::default();
    manifest_exact.max_manifest_bytes = manifest_len;
    append_seal(
        &r,
        "manifest-exact",
        &[event(1, vec![1])],
        manifest_exact.clone(),
    );
    assert!(load(&r, "manifest-exact", &manifest_exact).is_ok());
    let mut too_small = limits;
    too_small.max_record_bytes -= 1;
    assert!(matches!(
        load(&r, "boundary", &too_small),
        Err(Error::Corrupt(_))
    ));
    fs::remove_dir_all(r).unwrap()
}
#[test]
fn availability_extrema_use_all_records() {
    let r = root();
    let mut a = event(1, vec![1]);
    let mut b = event(2, vec![2]);
    let mut c = event(3, vec![3]);
    let received = Utc.timestamp_opt(1, 0).single().unwrap();
    a.received_at = received;
    b.received_at = received;
    c.received_at = received;
    a.available_at = Utc.timestamp_opt(20, 0).single().unwrap();
    b.available_at = Utc.timestamp_opt(30, 0).single().unwrap();
    c.available_at = Utc.timestamp_opt(10, 0).single().unwrap();
    append_seal(&r, "extrema", &[a, b, c], Limits::default());
    let (m, _) = load(&r, "extrema", &Limits::default()).unwrap();
    assert_eq!(
        m.min_available_at,
        Utc.timestamp_opt(10, 0).single().unwrap()
    );
    assert_eq!(
        m.max_available_at,
        Utc.timestamp_opt(30, 0).single().unwrap()
    );
    fs::remove_dir_all(r).unwrap()
}
#[test]
fn duplicate_backward_skip_mixed_identity_and_decreasing_elapsed_rejected() {
    for (name, mut bad) in [
        ("dup", event(1, vec![2])),
        ("back", event(4, vec![2])),
        ("skip", event(3, vec![2])),
        ("mixed", event(2, vec![2])),
        ("session", event(2, vec![2])),
        ("elapsed", event(2, vec![2])),
    ] {
        let r = root();
        let mut w = Writer::create(&r, name, Limits::default()).unwrap();
        if name == "back" {
            w.append(&event(5, vec![1])).unwrap();
        } else {
            w.append(&event(1, vec![1])).unwrap();
        }
        if name == "mixed" {
            bad.collector_id = "other".into()
        }
        if name == "session" {
            bad.collector_session_id = "other".into()
        }
        if name == "elapsed" {
            bad.monotonic_elapsed_nanos = 0;
        }
        assert!(matches!(w.append(&bad), Err(Error::Invalid(_))));
        fs::remove_dir_all(r).unwrap()
    }
}
#[test]
fn unknown_dropped_partition_is_preserved_and_empty_cannot_seal() {
    let r = root();
    let mut w = Writer::create(&r, "partial", Limits::default()).unwrap();
    assert!(matches!(w.seal(), Err(Error::Invalid(_))));
    w.append(&event(1, vec![1])).unwrap();
    drop(w);
    assert!(matches!(
        inspect(&r, "partial", &Limits::default()).unwrap(),
        Inspection::Uncommitted
    ));
    assert!(r.join("partial/events.jsonl").exists());
    fs::remove_dir_all(r).unwrap()
}
#[test]
fn invalid_payload_checksum_and_content_mutation_return_no_records() {
    let r = root();
    let mut e = event(1, vec![1]);
    e.payload_sha256 = "00".repeat(32);
    let mut w = Writer::create(&r, "bad", Limits::default()).unwrap();
    assert!(matches!(w.append(&e), Err(Error::Invalid(_))));
    drop(w);
    fs::remove_dir_all(r).unwrap();
    let r = root();
    append_seal(
        &r,
        "tamper",
        &[event(1, vec![1]), event(2, vec![2])],
        Limits::default(),
    );
    let p = r.join("tamper/events.jsonl");
    let mut raw = fs::read(&p).unwrap();
    let idx = raw.iter().position(|b| *b == b'[').unwrap();
    raw[idx + 1] = b'9';
    fs::write(p, raw).unwrap();
    assert!(matches!(
        load(&r, "tamper", &Limits::default()),
        Err(Error::Corrupt(_))
    ));
    fs::remove_dir_all(r).unwrap()
}
#[test]
fn preexisting_manifest_is_never_replaced_on_seal_failure() {
    let r = root();
    let mut w = Writer::create(&r, "race", Limits::default()).unwrap();
    w.append(&event(1, vec![1])).unwrap();
    let marker = b"preserve-marker";
    fs::write(r.join("race/manifest.json"), marker).unwrap();
    assert!(matches!(w.seal(), Err(Error::Io(_))));
    assert!(matches!(w.append(&event(2, vec![2])), Err(Error::Poisoned)));
    assert_eq!(fs::read(r.join("race/manifest.json")).unwrap(), marker);
    assert!(r.join("race/manifest.tmp").exists());
    fs::remove_dir_all(r).unwrap();
}
#[test]
fn committed_append_and_recreate_are_refused() {
    let r = root();
    let mut sealed = Writer::create(&r, "locked", Limits::default()).unwrap();
    sealed.append(&event(1, vec![1])).unwrap();
    sealed.seal().unwrap();
    assert!(matches!(
        sealed.append(&event(2, vec![2])),
        Err(Error::Sealed)
    ));
    assert!(matches!(sealed.seal(), Err(Error::Sealed)));
    let before = fs::read(r.join("locked/events.jsonl")).unwrap();
    assert!(Writer::create(&r, "locked", Limits::default()).is_err());
    assert_eq!(fs::read(r.join("locked/events.jsonl")).unwrap(), before);
    fs::remove_dir_all(r).unwrap()
}
#[test]
fn u64_max_single_record_valid_next_append_rejected() {
    let r = root();
    append_seal(&r, "max", &[event(u64::MAX, vec![1])], Limits::default());
    assert!(load(&r, "max", &Limits::default()).is_ok());
    let mut w = Writer::create(&r, "overflow", Limits::default()).unwrap();
    w.append(&event(u64::MAX, vec![1])).unwrap();
    let mut next = event(1, vec![2]);
    next.monotonic_elapsed_nanos = u64::MAX;
    assert!(matches!(w.append(&next), Err(Error::Invalid(_))));
    fs::remove_dir_all(r).unwrap()
}
#[test]
fn missing_manifest_is_uncommitted_missing_event_under_manifest_corrupt() {
    let r = root();
    fs::create_dir(r.join("missing")).unwrap();
    assert!(matches!(
        inspect(&r, "missing", &Limits::default()).unwrap(),
        Inspection::Uncommitted
    ));
    let mut w = Writer::create(&r, "gone", Limits::default()).unwrap();
    w.append(&event(1, vec![1])).unwrap();
    w.seal().unwrap();
    fs::remove_file(r.join("gone/events.jsonl")).unwrap();
    assert!(matches!(
        inspect(&r, "gone", &Limits::default()).unwrap(),
        Inspection::Corrupt(_)
    ));
    fs::remove_dir_all(r).unwrap()
}
#[test]
fn each_writer_cap_rejects_over_limit_and_reader_enforces_payload_count_bytes() {
    let r = root();
    let e = event(1, vec![1, 2]);
    for (which, cap) in [(0, 1u64), (1, 10), (2, 1)] {
        let mut l = Limits::default();
        match which {
            0 => l.max_payload_bytes = cap,
            1 => l.max_record_bytes = cap,
            2 => l.max_partition_bytes = cap,
            _ => unreachable!(),
        };
        let name = format!("limit{which}");
        let mut w = Writer::create(&r, &name, l).unwrap();
        assert!(matches!(w.append(&e), Err(Error::Limit(_))));
    }
    let mut count_limit = Limits::default();
    count_limit.max_records = 1;
    let mut w = Writer::create(&r, "count", count_limit).unwrap();
    w.append(&event(1, vec![1])).unwrap();
    assert!(matches!(w.append(&event(2, vec![2])), Err(Error::Limit(_))));
    drop(w);
    let mut manifest_limit = Limits::default();
    manifest_limit.max_manifest_bytes = 1;
    let mut w = Writer::create(&r, "manifest", manifest_limit).unwrap();
    w.append(&event(1, vec![1])).unwrap();
    assert!(matches!(w.seal(), Err(Error::Limit(_))));
    let mut l = Limits::default();
    l.max_records = 1;
    append_seal(&r, "one", &[event(1, vec![1])], l.clone());
    assert!(matches!(load(&r,"one",&l),Ok((_,v)) if v.len()==1));
    let mut over = l;
    over.max_records = 0;
    assert!(matches!(load(&r, "one", &over), Err(Error::Invalid(_))));
    fs::remove_dir_all(r).unwrap();
}
#[test]
fn malformed_json_truncation_and_missing_newline_are_corrupt() {
    for content in [b"{\"nope\"".as_slice(), b"{}\n{".as_slice()] {
        let r = root();
        append_seal(&r, "bad", &[event(1, vec![1])], Limits::default());
        fs::write(r.join("bad/events.jsonl"), content).unwrap();
        assert!(matches!(
            load(&r, "bad", &Limits::default()),
            Err(Error::Corrupt(_))
        ));
        fs::remove_dir_all(r).unwrap();
    }
}
#[test]
fn symlink_data_rejected_and_regular_root_required() {
    let r = root();
    let link = r.join("link");
    #[cfg(unix)]
    std::os::unix::fs::symlink("target", &link).unwrap();
    #[cfg(unix)]
    assert!(matches!(
        Writer::create(&r, "link", Limits::default()),
        Err(Error::Io(_))
    ));
    let file = r.join("not-dir");
    fs::write(&file, b"x").unwrap();
    assert!(matches!(
        load(&file, "x", &Limits::default()),
        Err(Error::Invalid(_))
    ));
    fs::remove_dir_all(r).unwrap();
}
