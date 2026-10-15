#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::field_reassign_with_default
)]
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
        "pm-record-corrupt-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&p).unwrap();
    p
}
fn ev(seq: u64) -> EventEnvelope {
    let t = Utc.timestamp_opt(1_800_000_000, 0).single().unwrap();
    let p = vec![1];
    EventEnvelope {
        schema_version: 1,
        source: "s".into(),
        stream: "x".into(),
        collector_id: "c".into(),
        collector_region: "unknown".into(),
        connection_id: "x".into(),
        collector_session_id: "sess".into(),
        ingest_sequence: seq,
        source_sequence: None,
        source_event_time: None,
        source_publish_time: None,
        received_at: t,
        available_at: t,
        monotonic_elapsed_nanos: seq,
        provider_time_unit: None,
        kind: EventKind::Gap,
        raw_payload: p.clone(),
        quality_flags: vec![],
        payload_sha256: sha256(&p),
        clock_status: ClockStatus::Unknown,
    }
}
fn committed(r: &PathBuf, id: &str, n: u64) {
    let mut w = Writer::create(r, id, Limits::default()).unwrap();
    for seq in 1..=n {
        w.append(&ev(seq)).unwrap()
    }
    w.seal().unwrap();
}
fn edit_manifest(r: &std::path::Path, id: &str, key: &str, value: serde_json::Value) {
    let p = r.join(id).join("manifest.json");
    let mut m: serde_json::Value = serde_json::from_slice(&fs::read(&p).unwrap()).unwrap();
    m[key] = value;
    fs::write(p, serde_json::to_vec(&m).unwrap()).unwrap()
}
#[test]
fn manifest_mutations_all_rejected_without_records() {
    let changes: Vec<(&str, serde_json::Value)> = vec![
        ("format_version", serde_json::json!(2)),
        ("partition_id", serde_json::json!("elsewhere")),
        ("sha256", serde_json::json!("00".repeat(32))),
        ("record_count", serde_json::json!(2)),
        ("event_bytes", serde_json::json!(1)),
        ("first_ingest_sequence", serde_json::json!(2)),
        ("last_ingest_sequence", serde_json::json!(9)),
        ("collector_id", serde_json::json!("other")),
        ("collector_session_id", serde_json::json!("other")),
        (
            "min_available_at",
            serde_json::json!("2030-01-01T00:00:00Z"),
        ),
        (
            "max_available_at",
            serde_json::json!("2000-01-01T00:00:00Z"),
        ),
    ];
    for (key, value) in changes {
        let r = root();
        committed(&r, "p", 1);
        edit_manifest(&r, "p", key, value);
        assert!(
            matches!(load(&r, "p", &Limits::default()), Err(Error::Corrupt(_))),
            "{key}"
        );
        fs::remove_dir_all(r).unwrap()
    }
}
#[test]
fn forged_sequence_range_overflow_is_corrupt_not_panic() {
    let r = root();
    committed(&r, "p", 1);
    let mut first = ev(u64::MAX);
    first.monotonic_elapsed_nanos = u64::MAX;
    let second = ev(1);
    let mut bytes = serde_json::to_vec(&first).unwrap();
    bytes.push(b'\n');
    bytes.extend(serde_json::to_vec(&second).unwrap());
    bytes.push(b'\n');
    rewrite_events_and_hash(&r, "p", &bytes);
    let mut m: serde_json::Value =
        serde_json::from_slice(&fs::read(r.join("p/manifest.json")).unwrap()).unwrap();
    m["first_ingest_sequence"] = serde_json::json!(u64::MAX);
    m["last_ingest_sequence"] = serde_json::json!(1);
    m["record_count"] = serde_json::json!(2);
    fs::write(r.join("p/manifest.json"), serde_json::to_vec(&m).unwrap()).unwrap();
    assert!(matches!(
        load(&r, "p", &Limits::default()),
        Err(Error::Corrupt(_))
    ));
    fs::remove_dir_all(r).unwrap()
}
fn rewrite_events_and_hash(r: &std::path::Path, id: &str, bytes: &[u8]) {
    fs::write(r.join(id).join("events.jsonl"), bytes).unwrap();
    let p = r.join(id).join("manifest.json");
    let mut m: serde_json::Value = serde_json::from_slice(&fs::read(&p).unwrap()).unwrap();
    m["event_bytes"] = serde_json::json!(bytes.len());
    m["sha256"] = serde_json::json!(sha256(bytes));
    fs::write(p, serde_json::to_vec(&m).unwrap()).unwrap();
}
#[test]
fn malformed_truncated_and_valid_json_without_newline_reach_parse_gates() {
    for kind in ["invalid", "truncated", "newline"] {
        let r = root();
        committed(&r, "p", 1);
        let original = fs::read(r.join("p/events.jsonl")).unwrap();
        let replacement = match kind {
            "invalid" => b"{bad json}\n".to_vec(),
            "truncated" => {
                let mut x = original.clone();
                x.remove(x.len() - 2);
                x
            }
            _ => original[..original.len() - 1].to_vec(),
        };
        rewrite_events_and_hash(&r, "p", &replacement);
        match load(&r, "p", &Limits::default()) {
            Err(Error::Corrupt(detail)) => {
                assert_ne!(detail, "event byte size mismatch");
                if kind == "newline" {
                    assert!(detail.contains("missing newline"));
                } else {
                    assert_ne!(detail, "missing newline");
                }
            }
            other => panic!("{kind}: unexpected {other:?}"),
        }
        fs::remove_dir_all(r).unwrap();
    }
}
#[test]
fn availability_manifest_wrong_but_coherent_extrema_fail_content_check() {
    let r = root();
    committed(&r, "p", 2);
    let mtime = "1999-01-01T00:00:00Z";
    edit_manifest(&r, "p", "min_available_at", serde_json::json!(mtime));
    edit_manifest(
        &r,
        "p",
        "max_available_at",
        serde_json::json!("2040-01-01T00:00:00Z"),
    );
    assert!(
        matches!(load(&r,"p",&Limits::default()),Err(Error::Corrupt(s)) if s.contains("manifest/content mismatch"))
    );
    fs::remove_dir_all(r).unwrap()
}
#[test]
fn valid_envelopes_with_wrong_second_record_identity_or_order_fail_record_gate() {
    for kind in [
        "duplicate",
        "skip",
        "backward",
        "collector",
        "session",
        "monotonic",
    ] {
        let r = root();
        committed(&r, "p", 2);
        let path = r.join("p/events.jsonl");
        let source = fs::read(&path).unwrap();
        let lines: Vec<&[u8]> = source
            .split(|b| *b == b'\n')
            .filter(|l| !l.is_empty())
            .collect();
        let mut a: EventEnvelope = serde_json::from_slice(lines[0]).unwrap();
        let mut b: EventEnvelope = serde_json::from_slice(lines[1]).unwrap();
        match kind {
            "duplicate" => b.ingest_sequence = 1,
            "skip" => b.ingest_sequence = 4,
            "backward" => {
                a.ingest_sequence = 2;
                b.ingest_sequence = 1;
            }
            "collector" => b.collector_id = "other".into(),
            "session" => b.collector_session_id = "other".into(),
            _ => b.monotonic_elapsed_nanos = 0,
        }
        let mut bytes = serde_json::to_vec(&a).unwrap();
        bytes.push(b'\n');
        bytes.extend(serde_json::to_vec(&b).unwrap());
        bytes.push(b'\n');
        rewrite_events_and_hash(&r, "p", &bytes);
        edit_manifest(
            &r,
            "p",
            "first_ingest_sequence",
            serde_json::json!(a.ingest_sequence),
        );
        edit_manifest(
            &r,
            "p",
            "last_ingest_sequence",
            serde_json::json!(b.ingest_sequence),
        );
        let result = load(&r, "p", &Limits::default());
        assert!(
            matches!(result,Err(Error::Corrupt(ref s)) if s.contains("ordering")||s.contains("identity")),
            "{kind}: {result:?}"
        );
        fs::remove_dir_all(r).unwrap();
    }
}
#[test]
fn reader_rejects_more_events_than_valid_claim_and_never_returns_prefix() {
    let r = root();
    committed(&r, "p", 2);
    edit_manifest(&r, "p", "record_count", serde_json::json!(1));
    edit_manifest(&r, "p", "last_ingest_sequence", serde_json::json!(1));
    assert!(matches!(
        load(&r, "p", &Limits::default()),
        Err(Error::Corrupt(_))
    ));
    fs::remove_dir_all(r).unwrap()
}
#[test]
fn reader_rejects_oversized_manifest_and_record() {
    let r = root();
    committed(&r, "p", 1);
    let mut l = Limits::default();
    l.max_manifest_bytes = 1;
    assert!(matches!(load(&r, "p", &l), Err(Error::Corrupt(_))));
    l = Limits::default();
    l.max_record_bytes = 1;
    assert!(matches!(load(&r, "p", &l), Err(Error::Corrupt(_))));
    fs::remove_dir_all(r).unwrap()
}
#[test]
fn payload_checksum_change_with_recomputed_partition_hash_is_rejected() {
    let r = root();
    committed(&r, "p", 1);
    let ep = r.join("p/events.jsonl");
    let original = fs::read(&ep).unwrap();
    let mut e: serde_json::Value = serde_json::from_slice(&original[..original.len() - 1]).unwrap();
    e["payload_sha256"] = serde_json::json!("00".repeat(32));
    let mut bytes = serde_json::to_vec(&e).unwrap();
    bytes.push(b'\n');
    fs::write(&ep, &bytes).unwrap();
    let mp = r.join("p/manifest.json");
    let mut m: serde_json::Value = serde_json::from_slice(&fs::read(&mp).unwrap()).unwrap();
    m["event_bytes"] = serde_json::json!(bytes.len());
    m["sha256"] = serde_json::json!(sha256(&bytes));
    fs::write(mp, serde_json::to_vec(&m).unwrap()).unwrap();
    assert!(matches!(
        load(&r, "p", &Limits::default()),
        Err(Error::Corrupt(_))
    ));
    fs::remove_dir_all(r).unwrap()
}
#[test]
fn reader_rejects_each_positive_but_too_small_limit() {
    let r = root();
    let mut w = Writer::create(&r, "p", Limits::default()).unwrap();
    let mut e = ev(1);
    e.raw_payload = vec![1, 2, 3];
    e.payload_sha256 = sha256(&e.raw_payload);
    w.append(&e).unwrap();
    let mut second = ev(2);
    second.raw_payload = vec![1, 2, 3];
    second.payload_sha256 = sha256(&second.raw_payload);
    w.append(&second).unwrap();
    w.seal().unwrap();
    let mut l = Limits::default();
    l.max_payload_bytes = 2;
    assert!(matches!(load(&r, "p", &l), Err(Error::Corrupt(_))));
    l = Limits::default();
    l.max_record_bytes = 1;
    assert!(matches!(load(&r, "p", &l), Err(Error::Corrupt(_))));
    l = Limits::default();
    l.max_partition_bytes = 1;
    assert!(matches!(load(&r, "p", &l), Err(Error::Corrupt(_))));
    l = Limits::default();
    l.max_records = 1;
    assert!(matches!(load(&r, "p", &l), Err(Error::Corrupt(_))));
    l = Limits::default();
    l.max_manifest_bytes = 1;
    assert!(matches!(load(&r, "p", &l), Err(Error::Corrupt(_))));
    fs::remove_dir_all(r).unwrap()
}
