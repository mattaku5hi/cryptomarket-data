#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::field_reassign_with_default,
    clippy::indexing_slicing
)]
use super::recording::{Limits, Scope, Writer};
use chrono::{TimeZone, Utc};
use pm_schema::{ClockStatus, EventEnvelope, EventKind, sha256};
use sha2::{Digest, Sha256};
use std::{
    fs,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
static N: AtomicUsize = AtomicUsize::new(0);
fn artifact_snapshot(root: &std::path::Path) -> Vec<(String, u8, Vec<u8>)> {
    fn walk(root: &std::path::Path, path: &std::path::Path, out: &mut Vec<(String, u8, Vec<u8>)>) {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let p = entry.path();
            let rel = p.strip_prefix(root).unwrap().to_string_lossy().into_owned();
            let meta = fs::symlink_metadata(&p).unwrap();
            if meta.file_type().is_symlink() {
                out.push((
                    rel,
                    2,
                    fs::read_link(&p)
                        .unwrap()
                        .to_string_lossy()
                        .as_bytes()
                        .to_vec(),
                ));
            } else if meta.is_dir() {
                out.push((rel.clone(), 1, Vec::new()));
                walk(root, &p, out);
            } else {
                out.push((rel, 0, fs::read(p).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}
fn event(seq: u64) -> EventEnvelope {
    let t = Utc.timestamp_opt(1_800_000_000, 0).single().unwrap();
    let raw_payload = vec![1];
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
        raw_payload: raw_payload.clone(),
        quality_flags: vec![],
        payload_sha256: sha256(&raw_payload),
        clock_status: ClockStatus::Unknown,
    }
}
fn event_at(seq: u64, elapsed: u64) -> EventEnvelope {
    let mut event = event(seq);
    event.monotonic_elapsed_nanos = elapsed;
    event
}
#[test]
fn byte_limit_rotation_and_completion_receipt() {
    let root = std::env::temp_dir().join(format!(
        "recording-test-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&root).unwrap();
    let mut limits = Limits::default();
    limits.max_partitions = 3;
    let event_size = u64::try_from(serde_json::to_vec(&event(1)).unwrap().len()).unwrap() + 1;
    limits.partition.max_partition_bytes = event_size;
    let mut w = Writer::create(
        &root,
        "r",
        Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into(),
        },
        limits.clone(),
    )
    .unwrap();
    assert!(w.append(&event(1)).unwrap().is_none());
    let rotated = w
        .append(&event(2))
        .unwrap()
        .expect("second event rotates exact-cap partition");
    assert_eq!(rotated.ordinal, 1);
    let start_bytes = fs::read(root.join("r/recording.json")).unwrap();
    let expected_start_sha = hex::encode(Sha256::digest(&start_bytes));
    let receipt = w.finish().unwrap();
    assert_eq!(receipt.recording_id, "r");
    assert_eq!(
        receipt.scope,
        Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into()
        }
    );
    assert_eq!(receipt.start_sha256, expected_start_sha);
    assert_eq!(receipt.records, 2);
    assert_eq!(receipt.partitions.len(), 2);
    assert!(matches!(w.append(&event(3)), Err(crate::Error::Sealed)));
    assert!(matches!(w.finish(), Err(crate::Error::Sealed)));
    let (first, loaded) = crate::load(root.join("r"), "p00000001", &limits.partition).unwrap();
    assert_eq!(first.record_count, 1);
    assert_eq!(
        serde_json::to_vec(&loaded[0]).unwrap(),
        serde_json::to_vec(&event(1)).unwrap()
    );
    let report = super::recording::inspect(&root, "r", &limits).unwrap();
    assert_eq!(report.recording_id, "r");
    assert_eq!(
        report.scope,
        Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into()
        }
    );
    assert_eq!(report.start_sha256, expected_start_sha);
    assert!(matches!(
        report.status,
        super::recording::Status::Complete(_)
    ));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn fresh_partition_receives_whole_event_larger_than_remaining_bytes() {
    let root = temp_root();
    let mut l = Limits::default();
    let first = event(1);
    let mut second = event(2);
    second.raw_payload = vec![7; 32];
    second.payload_sha256 = pm_schema::sha256(&second.raw_payload);
    l.partition.max_partition_bytes =
        u64::try_from(serde_json::to_vec(&second).unwrap().len()).unwrap() + 1;
    let mut w = writer(&root, l.clone());
    w.append(&first).unwrap();
    assert!(w.append(&second).unwrap().is_some());
    let r = w.finish().unwrap();
    assert_eq!(r.partitions.len(), 2);
    let (_, loaded) = crate::load(root.join("r"), "p00000002", &l.partition).unwrap();
    assert_eq!(
        serde_json::to_vec(&loaded[0]).unwrap(),
        serde_json::to_vec(&second).unwrap()
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn empty_finish_is_recoverable_and_completed_id_cannot_be_recreated() {
    let root = temp_root();
    let mut w = writer(&root, Limits::default());
    assert!(matches!(w.finish(), Err(crate::Error::Invalid(_))));
    assert!(!root.join("r/complete.json").exists());
    w.append(&event(1)).unwrap();
    let done = w.finish().unwrap();
    assert_eq!(done.records, 1);
    let snapshot = artifact_snapshot(&root.join("r"));
    assert!(
        Writer::create(
            &root,
            "r",
            Scope {
                collector_id: "c".into(),
                collector_region: "unknown".into(),
                session_id: "sess".into()
            },
            Limits::default()
        )
        .is_err()
    );
    assert_eq!(artifact_snapshot(&root.join("r")), snapshot);
    assert!(matches!(w.append(&event(2)), Err(crate::Error::Sealed)));
    assert!(matches!(w.finish(), Err(crate::Error::Sealed)));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn dropped_nonempty_first_tail_has_no_committed_prefix() {
    let root = temp_root();
    let mut w = writer(&root, Limits::default());
    w.append(&event(1)).unwrap();
    drop(w);
    let start = fs::read(root.join("r/recording.json")).unwrap();
    let expected_sha = hex::encode(Sha256::digest(&start));
    let before = artifact_snapshot(&root.join("r"));
    let report = super::recording::inspect(&root, "r", &Limits::default()).unwrap();
    assert_eq!(before, artifact_snapshot(&root.join("r")));
    assert_eq!(
        report.scope,
        Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into()
        }
    );
    assert_eq!(report.start_sha256, expected_sha);
    match report.status {
        super::recording::Status::Interrupted {
            committed,
            uncommitted_partition,
        } => {
            assert!(committed.is_empty());
            assert_eq!(uncommitted_partition.as_deref(), Some("p00000001"));
        }
        _ => panic!("nonempty tail is interrupted"),
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn sealed_prefix_with_nonempty_uncommitted_successor_is_interrupted() {
    let root = temp_root();
    let mut l = Limits::default();
    l.partition.max_partition_bytes = event_size();
    let mut w = writer(&root, l);
    w.append(&event(1)).unwrap();
    w.inject_next_partition_append_fault(crate::FaultAt::PartialAppend);
    assert!(w.append(&event(2)).is_err());
    assert!(matches!(w.append(&event(2)), Err(crate::Error::Poisoned)));
    assert!(matches!(w.finish(), Err(crate::Error::Poisoned)));
    let partial = fs::read(root.join("r/p00000002/events.jsonl")).unwrap();
    assert_eq!(partial.len(), 3);
    assert_ne!(partial.last(), Some(&b'\n'));
    assert!(!root.join("r/p00000002/manifest.json").exists());
    assert!(!root.join("r/p00000003").exists());
    drop(w);
    let before = artifact_snapshot(&root.join("r"));
    let report = super::recording::inspect(&root, "r", &Limits::default()).unwrap();
    assert_eq!(before, artifact_snapshot(&root.join("r")));
    assert_eq!(report.recording_id, "r");
    match report.status {
        super::recording::Status::Interrupted {
            committed,
            uncommitted_partition,
        } => {
            assert_eq!(committed.len(), 1);
            assert_eq!(
                committed[0],
                crate::load(root.join("r"), "p00000001", &Limits::default().partition)
                    .unwrap()
                    .0
            );
            assert_eq!(uncommitted_partition.as_deref(), Some("p00000002"));
        }
        _ => panic!("partial successor is uncommitted"),
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn partial_descriptor_and_completion_temps_are_not_authority() {
    let root = temp_root();
    let w = writer(&root, Limits::default());
    drop(w);
    let dir = root.join("r");
    let final_start = fs::read(dir.join("recording.json")).unwrap();
    fs::remove_file(dir.join("recording.json")).unwrap();
    fs::write(dir.join("recording.json"), b"{").unwrap();
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    fs::remove_file(dir.join("recording.json")).unwrap();
    fs::write(dir.join("recording.tmp"), b"{").unwrap();
    let before = artifact_snapshot(&dir);
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Uncommitted)
    ));
    assert_eq!(before, artifact_snapshot(&dir));
    fs::write(dir.join("recording.json"), final_start).unwrap();
    fs::remove_dir_all(root).unwrap();

    let root = temp_root();
    let mut w = writer(&root, Limits::default());
    w.append(&event(1)).unwrap();
    w.finish().unwrap();
    let dir = root.join("r");
    let completion = fs::read(dir.join("complete.json")).unwrap();
    fs::remove_file(dir.join("complete.json")).unwrap();
    fs::write(dir.join("complete.json"), b"{").unwrap();
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    fs::remove_file(dir.join("complete.json")).unwrap();
    fs::write(dir.join("complete.tmp"), b"{").unwrap();
    let before = artifact_snapshot(&dir);
    let report = super::recording::inspect(&root, "r", &Limits::default()).unwrap();
    assert_eq!(before, artifact_snapshot(&dir));
    assert_eq!(report.recording_id, "r");
    match report.status {
        super::recording::Status::Interrupted {
            committed,
            uncommitted_partition,
        } => {
            assert_eq!(committed.len(), 1);
            assert_eq!(uncommitted_partition, None);
        }
        _ => panic!("partial completion tmp has no authority"),
    }
    fs::write(dir.join("complete.json"), completion).unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn complete_marker_with_uncommitted_or_extra_committed_partition_rejected() {
    let root = temp_root();
    let mut w = writer(&root, Limits::default());
    w.append(&event(1)).unwrap();
    w.finish().unwrap();
    fs::create_dir(root.join("r/p00000002")).unwrap();
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    fs::remove_dir_all(root.join("r/p00000002")).unwrap();
    let mut p =
        crate::Writer::create(root.join("r"), "p00000002", crate::Limits::default()).unwrap();
    p.append(&event(2)).unwrap();
    p.seal().unwrap();
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn multiple_uncommitted_and_uncommitted_before_committed_are_errors() {
    let root = temp_root();
    let w = writer(&root, Limits::default());
    drop(w);
    for id in ["p00000001", "p00000002"] {
        let mut p = crate::Writer::create(root.join("r"), id, crate::Limits::default()).unwrap();
        p.append(&event(1)).unwrap();
        drop(p);
    }
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    fs::remove_dir_all(root).unwrap();
    let root = temp_root();
    let w = writer(&root, Limits::default());
    drop(w);
    let mut p1 =
        crate::Writer::create(root.join("r"), "p00000001", crate::Limits::default()).unwrap();
    p1.append(&event(1)).unwrap();
    drop(p1);
    let mut p2 =
        crate::Writer::create(root.join("r"), "p00000002", crate::Limits::default()).unwrap();
    p2.append(&event(1)).unwrap();
    p2.seal().unwrap();
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn root_and_partition_enumeration_limits_reject_extra_entries_bounded() {
    let root = temp_root();
    let mut w = writer(&root, Limits::default());
    w.append(&event(1)).unwrap();
    w.finish().unwrap();
    for n in 0..2 {
        fs::write(root.join("r").join(format!("extra{n}")), b"x").unwrap();
    }
    let mut l = Limits::default();
    l.max_partitions = 1;
    assert!(matches!(
        super::recording::inspect(&root, "r", &l),
        Err(crate::Error::Limit(_))
    ));
    fs::remove_dir_all(root).unwrap();

    let root = temp_root();
    let mut w = writer(&root, Limits::default());
    w.append(&event(1)).unwrap();
    w.finish().unwrap();
    fs::write(root.join("r/p00000001/extra"), b"x").unwrap();
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn known_artifacts_with_nonregular_types_are_rejected() {
    for artifact in [
        "recording.json",
        "recording.tmp",
        "complete.json",
        "complete.tmp",
        "events.jsonl",
        "manifest.json",
        "manifest.tmp",
    ] {
        let root = temp_root();
        let mut w = writer(&root, Limits::default());
        w.append(&event(1)).unwrap();
        w.finish().unwrap();
        let path = if artifact.contains(".jsonl") || artifact.starts_with("manifest") {
            root.join("r/p00000001").join(artifact)
        } else {
            root.join("r").join(artifact)
        };
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        assert!(
            matches!(
                super::recording::inspect(&root, "r", &Limits::default()),
                Err(crate::Error::Corrupt(_))
            ),
            "nonregular {artifact}"
        );
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn invalid_partition_ordinal_names_rejected() {
    for name in ["p00000000", "p0000001", "p100000000", "p99999999"] {
        let root = temp_root();
        let w = writer(&root, Limits::default());
        drop(w);
        fs::create_dir(root.join("r").join(name)).unwrap();
        assert!(
            super::recording::inspect(&root, "r", &Limits::default()).is_err(),
            "ordinal {name}"
        );
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn abandoned_new_set_is_interrupted_with_no_inferred_tail() {
    let root = temp_root();
    let w = writer(&root, Limits::default());
    drop(w);
    let start = fs::read(root.join("r/recording.json")).unwrap();
    let expected_sha = hex::encode(Sha256::digest(&start));
    let before = artifact_snapshot(&root.join("r"));
    let report = super::recording::inspect(&root, "r", &Limits::default()).unwrap();
    assert_eq!(before, artifact_snapshot(&root.join("r")));
    assert_eq!(report.recording_id, "r");
    assert_eq!(
        report.scope,
        Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into()
        }
    );
    assert_eq!(report.start_sha256, expected_sha);
    assert!(
        matches!(report.status,super::recording::Status::Interrupted{committed,uncommitted_partition:None} if committed.is_empty())
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn interrupted_committed_prefix_reports_only_final_uncommitted_partition() {
    let root = temp_root();
    let mut limits = Limits::default();
    limits.partition.max_partition_bytes = event_size();
    limits.max_partitions = 3;
    let mut w = writer(&root, limits.clone());
    w.append(&event(1)).unwrap();
    let sealed = w.append(&event(2)).unwrap().unwrap();
    assert_eq!(sealed.ordinal, 1);
    drop(w);
    let start = fs::read(root.join("r/recording.json")).unwrap();
    let expected_sha = hex::encode(Sha256::digest(&start));
    let before = artifact_snapshot(&root.join("r"));
    let report = super::recording::inspect(&root, "r", &limits).unwrap();
    assert_eq!(before, artifact_snapshot(&root.join("r")));
    assert_eq!(report.start_sha256, expected_sha);
    assert_eq!(report.recording_id, "r");
    assert_eq!(
        report.scope,
        Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into()
        }
    );
    match report.status {
        super::recording::Status::Interrupted {
            committed,
            uncommitted_partition,
        } => {
            assert_eq!(committed.len(), 1);
            assert_eq!(uncommitted_partition.as_deref(), Some("p00000002"));
        }
        _ => panic!("dropped tail is not complete"),
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn committed_prefix_without_completion_is_interrupted_and_missing_start_is_uncommitted() {
    let root = temp_root();
    let mut w = writer(&root, Limits::default());
    w.append(&event(1)).unwrap();
    w.finish().unwrap();
    fs::remove_file(root.join("r/complete.json")).unwrap();
    let before = artifact_snapshot(&root.join("r"));
    match super::recording::inspect(&root, "r", &Limits::default())
        .unwrap()
        .status
    {
        super::recording::Status::Interrupted {
            committed,
            uncommitted_partition,
        } => {
            assert_eq!(committed.len(), 1);
            assert_eq!(uncommitted_partition, None);
        }
        _ => panic!("without terminal marker set is interrupted"),
    }
    assert_eq!(before, artifact_snapshot(&root.join("r")));
    fs::remove_file(root.join("r/recording.json")).unwrap();
    let before_missing_start = artifact_snapshot(&root.join("r"));
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Uncommitted)
    ));
    assert_eq!(before_missing_start, artifact_snapshot(&root.join("r")));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn invalid_id_and_region_rejected_before_recording_directory_creation() {
    for (id, region) in [
        ("bad/id", "unknown"),
        ("r", "region name"),
        ("r", "region!"),
    ] {
        let root = temp_root();
        let scope = Scope {
            collector_id: "c".into(),
            collector_region: region.into(),
            session_id: "sess".into(),
        };
        assert!(Writer::create(&root, id, scope, Limits::default()).is_err());
        assert!(!root.join(id).exists());
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn invalid_preflight_is_recoverable_and_first_sequence_must_be_one() {
    let root = temp_root();
    let mut w = writer(&root, Limits::default());
    assert!(w.append(&event(2)).is_err());
    assert!(w.append(&event(1)).is_ok());
    assert!(w.finish().is_ok());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn count_limit_rotates_before_event_and_partition_count_cap_is_preflighted() {
    let root = temp_root();
    let mut l = Limits::default();
    l.partition.max_records = 1;
    l.max_partitions = 2;
    let mut w = writer(&root, l.clone());
    assert!(w.append(&event(1)).unwrap().is_none());
    assert_eq!(w.append(&event(2)).unwrap().unwrap().ordinal, 1);
    assert_eq!(w.finish().unwrap().partitions.len(), 2);
    fs::remove_dir_all(root).unwrap();
    let root = temp_root();
    let mut l = Limits::default();
    l.partition.max_partition_bytes = event_size();
    l.max_partitions = 1;
    let mut w = writer(&root, l);
    w.append(&event(1)).unwrap();
    let original = fs::read(root.join("r/p00000001/events.jsonl")).unwrap();
    assert!(w.append(&event(2)).is_err());
    assert_eq!(
        fs::read(root.join("r/p00000001/events.jsonl")).unwrap(),
        original
    );
    assert!(!root.join("r/p00000002").exists());
    assert_eq!(w.finish().unwrap().records, 1);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn partition_age_rotates_at_exact_monotonic_boundary() {
    let root = temp_root();
    let mut limits = Limits::default();
    limits.max_partitions = 2;
    let mut writer = Writer::create_with_partition_age(
        &root,
        "r",
        Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into(),
        },
        limits.clone(),
        Duration::from_nanos(10),
    )
    .unwrap();

    assert!(writer.append(&event_at(1, 100)).unwrap().is_none());
    assert!(writer.append(&event_at(2, 109)).unwrap().is_none());
    let rotated = writer
        .append(&event_at(3, 110))
        .unwrap()
        .expect("threshold event begins the next partition");
    assert_eq!(rotated.ordinal, 1);
    let receipt = writer.finish().unwrap();
    assert_eq!(receipt.records, 3);
    assert_eq!(receipt.partitions.len(), 2);

    let (_, first) = crate::load(root.join("r"), "p00000001", &limits.partition).unwrap();
    let (_, second) = crate::load(root.join("r"), "p00000002", &limits.partition).unwrap();
    assert_eq!(
        first
            .iter()
            .map(|event| event.ingest_sequence)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        second
            .iter()
            .map(|event| event.ingest_sequence)
            .collect::<Vec<_>>(),
        [3]
    );
    let report = super::recording::inspect(&root, "r", &limits).unwrap();
    assert!(matches!(
        report.status,
        super::recording::Status::Complete(_)
    ));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn partition_age_validation_and_idle_finish_do_not_create_empty_partitions() {
    let root = temp_root();
    let before = artifact_snapshot(&root);
    let scope = Scope {
        collector_id: "c".into(),
        collector_region: "unknown".into(),
        session_id: "sess".into(),
    };
    for age in [Duration::ZERO, Duration::new(u64::MAX, 1)] {
        assert!(matches!(
            Writer::create_with_partition_age(
                &root,
                "invalid-age",
                scope.clone(),
                Limits::default(),
                age,
            ),
            Err(crate::Error::Invalid(_))
        ));
        assert_eq!(artifact_snapshot(&root), before);
    }

    let limits = Limits::default();
    let mut writer = Writer::create_with_partition_age(
        &root,
        "r",
        scope,
        limits.clone(),
        Duration::from_nanos(10),
    )
    .unwrap();
    assert!(matches!(writer.finish(), Err(crate::Error::Invalid(_))));
    assert!(!root.join("r/p00000001").exists());
    writer.append(&event_at(1, 100)).unwrap();
    let receipt = writer.finish().unwrap();
    assert_eq!(receipt.partitions.len(), 1);
    let report = super::recording::inspect(&root, "r", &limits).unwrap();
    assert!(matches!(
        report.status,
        super::recording::Status::Complete(_)
    ));
    assert!(!root.join("r/p00000002").exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn partition_age_is_opt_in_and_monotonic_regression_preserves_writer() {
    let root = temp_root();
    let mut legacy = writer(&root, Limits::default());
    legacy.append(&event_at(1, 100)).unwrap();
    assert!(legacy.append(&event_at(2, 10_000)).unwrap().is_none());
    assert_eq!(legacy.finish().unwrap().partitions.len(), 1);
    fs::remove_dir_all(root).unwrap();

    let root = temp_root();
    let mut age_limited = Writer::create_with_partition_age(
        &root,
        "r",
        Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into(),
        },
        Limits::default(),
        Duration::from_nanos(100),
    )
    .unwrap();
    age_limited.append(&event_at(1, 100)).unwrap();
    assert!(age_limited.append(&event_at(2, 199)).unwrap().is_none());
    assert!(matches!(
        age_limited.append(&event_at(3, 198)),
        Err(crate::Error::Invalid(_))
    ));
    assert_eq!(
        age_limited
            .append(&event_at(3, 200))
            .unwrap()
            .unwrap()
            .ordinal,
        1
    );
    assert_eq!(age_limited.finish().unwrap().partitions.len(), 2);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn partition_age_origin_resets_after_count_and_byte_rotation() {
    let root = temp_root();
    let mut limits = Limits::default();
    limits.max_partitions = 3;
    limits.partition.max_records = 2;
    let mut writer = Writer::create_with_partition_age(
        &root,
        "r",
        Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into(),
        },
        limits.clone(),
        Duration::from_nanos(10),
    )
    .unwrap();
    writer.append(&event_at(1, 100)).unwrap();
    writer.append(&event_at(2, 104)).unwrap();
    assert_eq!(
        writer.append(&event_at(3, 108)).unwrap().unwrap().ordinal,
        1
    );
    assert!(writer.append(&event_at(4, 117)).unwrap().is_none());
    let receipt = writer.finish().unwrap();
    assert_eq!(receipt.partitions.len(), 2);
    fs::remove_dir_all(root).unwrap();

    let root = temp_root();
    let small = event_at(1, 100);
    let mut large = event_at(1, 100);
    large.raw_payload = vec![7; 1_024];
    large.payload_sha256 = sha256(&large.raw_payload);
    let small_bytes = u64::try_from(
        crate::encode_event(&small, &Limits::default().partition)
            .unwrap()
            .len(),
    )
    .unwrap();
    let large_bytes = u64::try_from(
        crate::encode_event(&large, &Limits::default().partition)
            .unwrap()
            .len(),
    )
    .unwrap();
    let cap = large_bytes.max(small_bytes * 2);
    assert!(large_bytes + small_bytes > cap);
    assert!(small_bytes * 2 <= cap);
    let mut limits = Limits::default();
    limits.max_partitions = 3;
    limits.partition.max_partition_bytes = cap;
    let mut writer = Writer::create_with_partition_age(
        &root,
        "r",
        Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into(),
        },
        limits,
        Duration::from_nanos(10),
    )
    .unwrap();
    assert!(writer.append(&large).unwrap().is_none());
    assert_eq!(
        writer.append(&event_at(2, 105)).unwrap().unwrap().ordinal,
        1
    );
    assert!(writer.append(&event_at(3, 114)).unwrap().is_none());
    assert_eq!(writer.finish().unwrap().partitions.len(), 2);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn partition_age_rotation_keeps_aggregate_and_partition_caps_preflighted() {
    for cap in ["bytes", "records", "partitions"] {
        let root = temp_root();
        let mut limits = Limits::default();
        if cap == "bytes" {
            limits.max_total_event_bytes = u64::try_from(
                crate::encode_event(&event_at(1, 100), &limits.partition)
                    .unwrap()
                    .len(),
            )
            .unwrap();
        } else if cap == "records" {
            limits.max_total_records = 1;
        } else {
            limits.max_partitions = 1;
        }
        let mut writer = Writer::create_with_partition_age(
            &root,
            "r",
            Scope {
                collector_id: "c".into(),
                collector_region: "unknown".into(),
                session_id: "sess".into(),
            },
            limits.clone(),
            Duration::from_nanos(10),
        )
        .unwrap();
        writer.append(&event_at(1, 100)).unwrap();
        assert!(matches!(
            writer.append(&event_at(2, 110)),
            Err(crate::Error::Limit(_))
        ));
        assert!(!root.join("r/p00000002").exists());
        let receipt = writer.finish().unwrap();
        assert_eq!(receipt.records, 1);
        assert_eq!(receipt.partitions.len(), 1);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn partition_age_boundary_seal_and_successor_append_failures_stay_interrupted() {
    let root = temp_root();
    let limits = Limits::default();
    let mut writer = Writer::create_with_partition_age(
        &root,
        "r",
        Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into(),
        },
        limits.clone(),
        Duration::from_nanos(10),
    )
    .unwrap();
    writer.append(&event_at(1, 100)).unwrap();
    writer.inject_partition_fault(crate::FaultAt::Flush);
    assert!(writer.append(&event_at(2, 110)).is_err());
    assert!(matches!(
        writer.append(&event_at(2, 110)),
        Err(crate::Error::Poisoned)
    ));
    assert!(matches!(writer.finish(), Err(crate::Error::Poisoned)));
    drop(writer);
    let report = super::recording::inspect(&root, "r", &limits).unwrap();
    assert!(matches!(
        report.status,
        super::recording::Status::Interrupted {
            committed,
            uncommitted_partition: Some(ref id),
        } if committed.is_empty() && id == "p00000001"
    ));
    fs::remove_dir_all(root).unwrap();

    let root = temp_root();
    let mut writer = Writer::create_with_partition_age(
        &root,
        "r",
        Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into(),
        },
        limits.clone(),
        Duration::from_nanos(10),
    )
    .unwrap();
    writer.append(&event_at(1, 100)).unwrap();
    writer.inject_next_partition_append_fault(crate::FaultAt::PartialAppend);
    assert!(writer.append(&event_at(2, 110)).is_err());
    assert!(matches!(
        writer.append(&event_at(2, 110)),
        Err(crate::Error::Poisoned)
    ));
    assert!(matches!(writer.finish(), Err(crate::Error::Poisoned)));
    drop(writer);
    let report = super::recording::inspect(&root, "r", &limits).unwrap();
    assert!(matches!(
        report.status,
        super::recording::Status::Interrupted {
            committed,
            uncommitted_partition: Some(ref id),
        } if committed.len() == 1
            && committed[0].first_ingest_sequence == 1
            && id == "p00000002"
    ));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn pinned_region_and_monotonic_sequence_preflight_do_not_create_successor() {
    for kind in 0..3 {
        let root = temp_root();
        let mut l = Limits::default();
        l.partition.max_partition_bytes = event_size();
        let mut w = writer(&root, l);
        w.append(&event(1)).unwrap();
        let mut invalid = event(if kind == 1 { 3 } else { 2 });
        if kind == 0 {
            invalid.collector_region = "other".into();
        }
        if kind == 2 {
            invalid.monotonic_elapsed_nanos = 0;
        }
        assert!(w.append(&invalid).is_err());
        assert!(!root.join("r/p00000002").exists());
        assert_eq!(w.finish().unwrap().records, 1);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn invalid_event_preflight_preserves_open_partition_and_valid_retry_rotates() {
    for kind in [
        "schema",
        "hash",
        "duplicate",
        "backwards",
        "gap",
        "collector",
        "session",
        "region",
        "mono",
    ] {
        let root = temp_root();
        let stage = if kind == "backwards" { 2 } else { 1 };
        let mut l = Limits::default();
        l.partition.max_partition_bytes = event_size() * stage;
        let mut w = writer(&root, l);
        for seq in 1..=stage {
            w.append(&event(seq)).unwrap();
        }
        let before = fs::read(root.join("r/p00000001/events.jsonl")).unwrap();
        let invalid_seq = match kind {
            "gap" => 3,
            "backwards" => 1,
            "duplicate" => 1,
            _ => stage + 1,
        };
        let mut bad = event(invalid_seq);
        match kind {
            "schema" => bad.schema_version = 99,
            "hash" => bad.payload_sha256 = "0".repeat(64),
            "collector" => bad.collector_id = "other".into(),
            "session" => bad.collector_session_id = "other".into(),
            "region" => bad.collector_region = "other".into(),
            "mono" => bad.monotonic_elapsed_nanos = 0,
            _ => {}
        }
        assert!(w.append(&bad).is_err(), "preflight {kind}");
        assert_eq!(
            fs::read(root.join("r/p00000001/events.jsonl")).unwrap(),
            before
        );
        assert!(!root.join("r/p00000001/manifest.json").exists());
        assert!(!root.join("r/p00000002").exists());
        assert_eq!(w.append(&event(stage + 1)).unwrap().unwrap().ordinal, 1);
        assert_eq!(w.finish().unwrap().records, stage + 1);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn serialized_record_limit_is_independent_and_preflight_retry_succeeds() {
    let root = temp_root();
    let first = event(1);
    let mut one_byte_over = event(2);
    one_byte_over.source.push('x');
    let exact = u64::try_from(serde_json::to_vec(&first).unwrap().len() + 1).unwrap();
    let one_over = u64::try_from(serde_json::to_vec(&one_byte_over).unwrap().len() + 1).unwrap();
    assert_eq!(one_over, exact + 1);
    one_byte_over.validate().unwrap();
    let mut l = Limits::default();
    assert!(
        u64::try_from(one_byte_over.raw_payload.len()).unwrap() <= l.partition.max_payload_bytes
    );
    l.partition.max_record_bytes = exact;
    l.partition.max_partition_bytes = 64 * 1024;
    l.partition.max_records = 1;
    assert!(one_over <= l.partition.max_partition_bytes);
    let mut w = writer(&root, l);
    w.append(&first).unwrap();
    let before = fs::read(root.join("r/p00000001/events.jsonl")).unwrap();
    assert!(matches!(
        w.append(&one_byte_over),
        Err(crate::Error::Limit(_))
    ));
    assert_eq!(
        fs::read(root.join("r/p00000001/events.jsonl")).unwrap(),
        before
    );
    assert!(!root.join("r/p00000001/manifest.json").exists());
    assert!(!root.join("r/p00000002").exists());
    assert!(w.append(&event(2)).unwrap().is_some());
    assert_eq!(w.finish().unwrap().records, 2);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn payload_exact_cap_and_plus_one_preflight_preserve_retry() {
    let root = temp_root();
    let mut l = Limits::default();
    l.partition.max_payload_bytes = 1;
    l.partition.max_record_bytes = 64 * 1024;
    l.partition.max_partition_bytes = 64 * 1024;
    l.partition.max_records = 1;
    assert_eq!(event(1).raw_payload.len(), 1);
    let mut w = writer(&root, l);
    w.append(&event(1)).unwrap();
    let before = fs::read(root.join("r/p00000001/events.jsonl")).unwrap();
    let mut too_big = event(2);
    too_big.raw_payload = vec![1, 2];
    too_big.payload_sha256 = pm_schema::sha256(&too_big.raw_payload);
    too_big.validate().unwrap();
    assert_eq!(too_big.raw_payload.len(), 2);
    let encoded_len = u64::try_from(serde_json::to_vec(&too_big).unwrap().len() + 1).unwrap();
    assert!(encoded_len <= 64 * 1024);
    assert!(matches!(w.append(&too_big), Err(crate::Error::Limit(_))));
    assert_eq!(
        fs::read(root.join("r/p00000001/events.jsonl")).unwrap(),
        before
    );
    assert!(!root.join("r/p00000001/manifest.json").exists());
    assert!(!root.join("r/p00000002").exists());
    assert!(w.append(&event(2)).unwrap().is_some());
    assert_eq!(w.finish().unwrap().records, 2);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn aggregate_byte_preflight_allows_smaller_same_sequence_retry() {
    let root = temp_root();
    let first = event(1);
    let mut too_large = event(2);
    too_large.quality_flags.push("large".repeat(20));
    let mut smaller = event(2);
    smaller.quality_flags.clear();
    let bytes =
        |e: &EventEnvelope| u64::try_from(serde_json::to_vec(e).unwrap().len() + 1).unwrap();
    let first_bytes = bytes(&first);
    let large_bytes = bytes(&too_large);
    let small_bytes = bytes(&smaller);
    let mut l = Limits::default();
    l.max_total_event_bytes = first_bytes + small_bytes;
    l.partition.max_partition_bytes = large_bytes;
    let mut w = writer(&root, l);
    w.append(&first).unwrap();
    let before = fs::read(root.join("r/p00000001/events.jsonl")).unwrap();
    assert!(matches!(w.append(&too_large), Err(crate::Error::Limit(_))));
    assert_eq!(
        fs::read(root.join("r/p00000001/events.jsonl")).unwrap(),
        before
    );
    assert!(!root.join("r/p00000001/manifest.json").exists());
    assert!(!root.join("r/p00000002").exists());
    assert!(w.append(&smaller).unwrap().is_some());
    assert_eq!(w.finish().unwrap().records, 2);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn single_event_partition_and_payload_overlimits_preflight_without_partition_creation() {
    for payload_limit in [false, true] {
        let root = temp_root();
        let mut l = Limits::default();
        if payload_limit {
            l.partition.max_payload_bytes = 1;
        } else {
            l.partition.max_partition_bytes = event_size() - 1;
        }
        let mut w = writer(&root, l);
        let mut oversized = event(1);
        if payload_limit {
            oversized.raw_payload = vec![1, 2];
            oversized.payload_sha256 = pm_schema::sha256(&oversized.raw_payload);
        }
        assert!(matches!(w.append(&oversized), Err(crate::Error::Limit(_))));
        assert!(!root.join("r/p00000001").exists());
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn aggregate_budget_rejects_before_any_rotation_and_finish_at_cap_works() {
    let root = temp_root();
    let mut limits = Limits::default();
    limits.max_total_event_bytes = event_size();
    limits.partition.max_partition_bytes = event_size();
    let mut w = writer(&root, limits.clone());
    w.append(&event(1)).unwrap();
    assert!(w.append(&event(2)).is_err());
    assert!(!root.join("r/p00000002").exists());
    assert_eq!(w.finish().unwrap().records, 1);
    fs::remove_dir_all(root).unwrap();
    let root = temp_root();
    let mut l = Limits::default();
    l.max_total_records = 1;
    let mut w = writer(&root, l);
    w.append(&event(1)).unwrap();
    assert!(matches!(w.append(&event(2)), Err(crate::Error::Limit(_))));
    assert_eq!(w.finish().unwrap().records, 1);
    fs::remove_dir_all(root).unwrap();
}

fn temp_root() -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "recording-test-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&root).unwrap();
    root
}
fn event_size() -> u64 {
    u64::try_from(serde_json::to_vec(&event(1)).unwrap().len()).unwrap() + 1
}
fn writer(root: &std::path::Path, limits: Limits) -> Writer {
    Writer::create(
        root,
        "r",
        Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into(),
        },
        limits,
    )
    .unwrap()
}

#[test]
fn reader_rechecks_each_caller_bound_and_uses_remaining_aggregate_budgets() {
    let root = temp_root();
    let mut base = Limits::default();
    base.partition.max_partition_bytes = 4096;
    base.partition.max_records = 2;
    base.max_partitions = 2;
    let mut w = writer(&root, base.clone());
    let mut evs = Vec::new();
    for n in 1..=3 {
        let mut e = event(n);
        e.raw_payload = vec![1, 2];
        e.payload_sha256 = pm_schema::sha256(&e.raw_payload);
        evs.push(e);
    }
    for e in &evs {
        w.append(e).unwrap();
    }
    let receipt = w.finish().unwrap();
    let total = receipt.event_bytes;
    let total_records = receipt.records;
    assert_eq!(receipt.partitions.len(), 2);
    let first_bytes = receipt.partitions[0].event_bytes;
    let first_records = receipt.partitions[0].record_count;
    let mut cases = Vec::new();
    let mut l = base.clone();
    l.max_metadata_bytes = 1;
    cases.push(l);
    let mut l = base.clone();
    l.partition.max_payload_bytes = 1;
    cases.push(l);
    let mut l = base.clone();
    l.partition.max_record_bytes =
        u64::try_from(serde_json::to_vec(&evs[0]).unwrap().len()).unwrap();
    cases.push(l);
    let mut l = base.clone();
    l.partition.max_partition_bytes = first_bytes - 1;
    cases.push(l);
    let mut l = base.clone();
    l.partition.max_records = 1;
    cases.push(l);
    let mut l = base.clone();
    l.max_total_event_bytes = total - 1;
    cases.push(l);
    let mut l = base.clone();
    l.max_total_records = total_records - 1;
    cases.push(l);
    let mut l = base.clone();
    l.max_partitions = 1;
    cases.push(l);
    let mut l = base.clone();
    l.partition.max_manifest_bytes = 1;
    cases.push(l);
    let before = artifact_snapshot(&root.join("r"));
    for (i, l) in cases.into_iter().enumerate() {
        assert!(
            super::recording::inspect(&root, "r", &l).is_err(),
            "limit case {i}"
        );
        assert_eq!(
            before,
            artifact_snapshot(&root.join("r")),
            "limit case {i} mutated artifacts"
        );
    }
    let mut bytes_exhausted = base.clone();
    bytes_exhausted.max_total_event_bytes = first_bytes;
    assert!(super::recording::inspect(&root, "r", &bytes_exhausted).is_err());
    assert_eq!(before, artifact_snapshot(&root.join("r")));
    let mut records_exhausted = base.clone();
    records_exhausted.max_total_records = first_records;
    assert!(super::recording::inspect(&root, "r", &records_exhausted).is_err());
    assert_eq!(before, artifact_snapshot(&root.join("r")));
    assert!(matches!(
        super::recording::inspect(&root, "r", &base).unwrap().status,
        super::recording::Status::Complete(_)
    ));
    let mut exact = base.clone();
    exact.max_total_event_bytes = total;
    exact.max_total_records = total_records;
    exact.partition.max_payload_bytes = 2;
    exact.partition.max_records = 2;
    exact.partition.max_partition_bytes = first_bytes;
    exact.partition.max_record_bytes =
        u64::try_from(serde_json::to_vec(&evs[0]).unwrap().len() + 1).unwrap();
    exact.partition.max_manifest_bytes = u64::try_from(
        receipt
            .partitions
            .iter()
            .map(|m| serde_json::to_vec(m).unwrap().len())
            .max()
            .unwrap(),
    )
    .unwrap();
    exact.max_metadata_bytes = ["recording.json", "complete.json"]
        .into_iter()
        .map(|n| fs::metadata(root.join("r").join(n)).unwrap().len())
        .max()
        .unwrap();
    assert!(matches!(
        super::recording::inspect(&root, "r", &exact)
            .unwrap()
            .status,
        super::recording::Status::Complete(_)
    ));
    assert_eq!(before, artifact_snapshot(&root.join("r")));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn on_disk_completion_reference_count_cannot_exceed_metadata_budget() {
    let root = temp_root();
    let mut w = writer(&root, Limits::default());
    w.append(&event(1)).unwrap();
    w.finish().unwrap();
    let path = root.join("r/complete.json");
    let mut end: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let reference = end["partitions"][0].clone();
    for _ in 0..40 {
        end["partitions"]
            .as_array_mut()
            .unwrap()
            .push(reference.clone());
    }
    fs::write(&path, serde_json::to_vec(&end).unwrap()).unwrap();
    let mut l = Limits::default();
    l.max_partitions = 1;
    l.max_metadata_bytes = 1024;
    let before = artifact_snapshot(&root.join("r"));
    assert!(super::recording::inspect(&root, "r", &l).is_err());
    assert_eq!(before, artifact_snapshot(&root.join("r")));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn maximum_format_bounds_and_identifier_lengths_are_checked_without_large_allocations() {
    let root = temp_root();
    let mut l = Limits::default();
    l.max_partitions = 99_999_999;
    let w = Writer::create(
        &root,
        "r",
        Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into(),
        },
        l.clone(),
    )
    .unwrap();
    drop(w);
    l.max_partitions = 100_000_000;
    assert!(
        Writer::create(
            &root,
            "x",
            Scope {
                collector_id: "c".into(),
                collector_region: "unknown".into(),
                session_id: "sess".into()
            },
            l
        )
        .is_err()
    );
    assert!(!root.join("x").exists());
    let id = "a".repeat(128);
    let scope = Scope {
        collector_id: id.clone(),
        collector_region: id.clone(),
        session_id: id.clone(),
    };
    let w = Writer::create(&root, &id, scope, Limits::default()).unwrap();
    drop(w);
    for which in 0..4 {
        let root2 = temp_root();
        let mut id = "r".to_string();
        let mut scope = Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into(),
        };
        match which {
            0 => id = "a".repeat(129),
            1 => scope.collector_id = "a".repeat(129),
            2 => scope.collector_region = "a".repeat(129),
            _ => scope.session_id = "a".repeat(129),
        };
        assert!(Writer::create(&root2, &id, scope, Limits::default()).is_err());
        fs::remove_dir_all(root2).unwrap();
    }
    let mut l = Limits::default();
    l.partition.max_manifest_bytes = u64::MAX;
    let root2 = temp_root();
    assert!(
        Writer::create(
            &root2,
            "r",
            Scope {
                collector_id: "c".into(),
                collector_region: "unknown".into(),
                session_id: "sess".into()
            },
            l
        )
        .is_err()
    );
    assert!(!root2.join("r").exists());
    fs::remove_dir_all(root2).unwrap();
    let root3 = temp_root();
    let mut l = Limits::default();
    l.max_metadata_bytes = u64::MAX;
    assert!(
        Writer::create(
            &root3,
            "r",
            Scope {
                collector_id: "c".into(),
                collector_region: "unknown".into(),
                session_id: "sess".into()
            },
            l
        )
        .is_err()
    );
    assert!(!root3.join("r").exists());
    fs::remove_dir_all(root3).unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn recording_sequence_overflow_is_preflighted_without_advancing_disk_state() {
    let root = temp_root();
    let mut w = writer(&root, Limits::default());
    w.append(&event(1)).unwrap();
    w.force_last_sequence_for_test(u64::MAX);
    let before = fs::read(root.join("r/p00000001/events.jsonl")).unwrap();
    assert!(matches!(w.append(&event(2)), Err(crate::Error::Invalid(_))));
    assert_eq!(
        fs::read(root.join("r/p00000001/events.jsonl")).unwrap(),
        before
    );
    drop(w);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn initial_partition_create_failure_poison_is_sticky() {
    let root = temp_root();
    let mut w = writer(&root, Limits::default());
    fs::create_dir(root.join("r/p00000001")).unwrap();
    assert!(matches!(w.append(&event(1)), Err(crate::Error::Io(_))));
    assert!(matches!(w.append(&event(1)), Err(crate::Error::Poisoned)));
    assert!(matches!(w.finish(), Err(crate::Error::Poisoned)));
    drop(w);
    let before = artifact_snapshot(&root.join("r"));
    let report = super::recording::inspect(&root, "r", &Limits::default()).unwrap();
    assert_eq!(before, artifact_snapshot(&root.join("r")));
    assert_eq!(report.recording_id, "r");
    assert_eq!(
        report.scope,
        Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into()
        }
    );
    match report.status {
        super::recording::Status::Interrupted {
            committed,
            uncommitted_partition,
        } => {
            assert!(committed.is_empty());
            assert_eq!(uncommitted_partition.as_deref(), Some("p00000001"));
        }
        _ => panic!("failed initial partition create is interrupted"),
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn finish_metadata_budget_failure_after_tail_seal_poisoned_without_completion() {
    let root = temp_root();
    let probe = writer(&root, Limits::default());
    let start_len = fs::metadata(root.join("r/recording.json")).unwrap().len();
    drop(probe);
    fs::remove_dir_all(root.join("r")).unwrap();
    let mut l = Limits::default();
    l.max_metadata_bytes = start_len;
    let mut w = writer(&root, l.clone());
    w.append(&event(1)).unwrap();
    assert!(matches!(w.finish(), Err(crate::Error::Limit(_))));
    assert!(matches!(w.append(&event(2)), Err(crate::Error::Poisoned)));
    assert!(matches!(w.finish(), Err(crate::Error::Poisoned)));
    drop(w);
    assert!(!root.join("r/complete.tmp").exists());
    assert!(!root.join("r/complete.json").exists());
    let before = artifact_snapshot(&root.join("r"));
    let report = super::recording::inspect(&root, "r", &l).unwrap();
    assert_eq!(before, artifact_snapshot(&root.join("r")));
    assert_eq!(report.recording_id, "r");
    assert_eq!(
        report.scope,
        Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into()
        }
    );
    match report.status {
        super::recording::Status::Interrupted {
            committed,
            uncommitted_partition,
        } => {
            assert_eq!(committed.len(), 1);
            assert_eq!(committed[0].partition_id, "p00000001");
            assert_eq!(uncommitted_partition, None);
        }
        _ => panic!("sealed prefix has no completion"),
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn successor_create_failure_keeps_committed_prefix_and_poisons_writer() {
    let root = temp_root();
    let mut l = Limits::default();
    l.partition.max_partition_bytes = event_size();
    let mut w = writer(&root, l);
    w.append(&event(1)).unwrap();
    fs::create_dir(root.join("r/p00000002")).unwrap();
    assert!(matches!(w.append(&event(2)), Err(crate::Error::Io(_))));
    assert!(matches!(w.append(&event(2)), Err(crate::Error::Poisoned)));
    assert!(matches!(w.finish(), Err(crate::Error::Poisoned)));
    assert!(
        fs::read_dir(root.join("r/p00000002"))
            .unwrap()
            .next()
            .is_none()
    );
    drop(w);
    let before = artifact_snapshot(&root.join("r"));
    let report = super::recording::inspect(&root, "r", &Limits::default()).unwrap();
    assert_eq!(before, artifact_snapshot(&root.join("r")));
    assert_eq!(report.recording_id, "r");
    assert_eq!(
        report.scope,
        Scope {
            collector_id: "c".into(),
            collector_region: "unknown".into(),
            session_id: "sess".into()
        }
    );
    match report.status {
        super::recording::Status::Interrupted {
            committed,
            uncommitted_partition,
        } => {
            assert_eq!(committed.len(), 1);
            assert_eq!(
                committed[0],
                crate::load(root.join("r"), "p00000001", &Limits::default().partition)
                    .unwrap()
                    .0
            );
            assert_eq!(uncommitted_partition.as_deref(), Some("p00000002"));
        }
        _ => panic!("failed successor cannot complete"),
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn partition_append_and_rotation_seal_failures_poison_without_success_receipt() {
    for point in [
        crate::FaultAt::PartialAppend,
        crate::FaultAt::Flush,
        crate::FaultAt::EventSync,
        crate::FaultAt::ManifestWrite,
        crate::FaultAt::ManifestSync,
        crate::FaultAt::Publish,
        crate::FaultAt::PartitionSync,
        crate::FaultAt::RootSync,
    ] {
        let root = temp_root();
        let mut limits = Limits::default();
        let rotating = point != crate::FaultAt::PartialAppend;
        limits.partition.max_partition_bytes = if rotating {
            event_size()
        } else {
            event_size() * 2
        };
        let mut w = writer(&root, limits);
        w.append(&event(1)).unwrap();
        let original_events = fs::read(root.join("r/p00000001/events.jsonl")).unwrap();
        w.inject_partition_fault(point);
        assert!(matches!(w.append(&event(2)), Err(crate::Error::Io(_))));
        let events_after = fs::read(root.join("r/p00000001/events.jsonl")).unwrap();
        if point == crate::FaultAt::PartialAppend {
            assert!(events_after.starts_with(&original_events));
            assert_eq!(events_after.len(), original_events.len() + 3);
            assert_ne!(events_after.last(), Some(&b'\n'));
        } else {
            assert_eq!(events_after, original_events);
        }
        let partition_dir = root.join("r/p00000001");
        let postpublish = matches!(
            point,
            crate::FaultAt::PartitionSync | crate::FaultAt::RootSync
        );
        assert_eq!(partition_dir.join("manifest.json").exists(), postpublish);
        let temp_manifest = partition_dir.join("manifest.tmp");
        assert_eq!(
            temp_manifest.exists(),
            matches!(
                point,
                crate::FaultAt::ManifestWrite
                    | crate::FaultAt::ManifestSync
                    | crate::FaultAt::Publish
                    | crate::FaultAt::PartitionSync
                    | crate::FaultAt::RootSync
            )
        );
        if point == crate::FaultAt::ManifestWrite {
            assert_eq!(fs::metadata(&temp_manifest).unwrap().len(), 0);
        }
        if matches!(
            point,
            crate::FaultAt::ManifestSync
                | crate::FaultAt::Publish
                | crate::FaultAt::PartitionSync
                | crate::FaultAt::RootSync
        ) {
            assert!(fs::metadata(&temp_manifest).unwrap().len() > 0);
        }
        if postpublish {
            assert_eq!(
                fs::read(&temp_manifest).unwrap(),
                fs::read(partition_dir.join("manifest.json")).unwrap()
            );
        }
        assert!(matches!(w.append(&event(2)), Err(crate::Error::Poisoned)));
        assert!(matches!(w.finish(), Err(crate::Error::Poisoned)));
        drop(w);
        assert!(!root.join("r/complete.json").exists());
        assert!(!root.join("r/p00000002").exists());
        let before = artifact_snapshot(&root.join("r"));
        let report = super::recording::inspect(&root, "r", &Limits::default()).unwrap();
        assert_eq!(before, artifact_snapshot(&root.join("r")));
        match (point, report.status) {
            (
                crate::FaultAt::PartitionSync | crate::FaultAt::RootSync,
                super::recording::Status::Interrupted {
                    committed,
                    uncommitted_partition,
                },
            ) => {
                assert_eq!(committed.len(), 1);
                assert_eq!(committed[0].partition_id, "p00000001");
                assert_eq!(
                    committed[0],
                    crate::load(root.join("r"), "p00000001", &Limits::default().partition)
                        .unwrap()
                        .0
                );
                assert_eq!(uncommitted_partition, None);
            }
            (
                _,
                super::recording::Status::Interrupted {
                    committed,
                    uncommitted_partition,
                },
            ) => {
                assert!(committed.is_empty());
                assert_eq!(uncommitted_partition.as_deref(), Some("p00000001"));
            }
            _ => panic!("partition fault recovery status"),
        }
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn every_zero_budget_and_metadata_overflow_rejected_before_directory_creation() {
    for field in 0..9 {
        let root = temp_root();
        let mut l = Limits::default();
        match field {
            0 => l.max_partitions = 0,
            1 => l.max_total_event_bytes = 0,
            2 => l.max_total_records = 0,
            3 => l.max_metadata_bytes = 0,
            4 => l.partition.max_payload_bytes = 0,
            5 => l.partition.max_record_bytes = 0,
            6 => l.partition.max_partition_bytes = 0,
            7 => l.partition.max_records = 0,
            _ => l.partition.max_manifest_bytes = 0,
        }
        assert!(
            Writer::create(
                &root,
                "r",
                Scope {
                    collector_id: "c".into(),
                    collector_region: "unknown".into(),
                    session_id: "sess".into()
                },
                l
            )
            .is_err()
        );
        assert!(!root.join("r").exists());
        fs::remove_dir_all(root).unwrap();
    }
    let root = temp_root();
    let mut l = Limits::default();
    l.max_metadata_bytes = u64::MAX;
    assert!(
        Writer::create(
            &root,
            "r",
            Scope {
                collector_id: "c".into(),
                collector_region: "unknown".into(),
                session_id: "sess".into()
            },
            l
        )
        .is_err()
    );
    assert!(!root.join("r").exists());
    fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn recording_and_partition_symlinks_are_rejected() {
    use std::os::unix::fs::symlink;
    let root = temp_root();
    let mut w = writer(&root, Limits::default());
    w.append(&event(1)).unwrap();
    w.finish().unwrap();
    let events = root.join("r/p00000001/events.jsonl");
    let target = root.join("outside");
    fs::write(&target, b"x").unwrap();
    fs::remove_file(&events).unwrap();
    symlink(&target, &events).unwrap();
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    fs::remove_dir_all(root.join("r")).unwrap();
    symlink(&target, root.join("r")).unwrap();
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    fs::remove_file(root.join("r")).unwrap();
    fs::remove_file(target).unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn every_metadata_partition_directory_and_data_symlink_is_rejected() {
    use std::os::unix::fs::symlink;
    for artifact in [
        "recording.json",
        "recording.tmp",
        "complete.json",
        "complete.tmp",
        "partition-dir",
        "events.jsonl",
        "manifest.json",
        "manifest.tmp",
    ] {
        let root = temp_root();
        let mut w = writer(&root, Limits::default());
        w.append(&event(1)).unwrap();
        w.finish().unwrap();
        let set = root.join("r");
        let target = root.join("outside");
        fs::write(&target, b"external").unwrap();
        let path = match artifact {
            "recording.json" | "recording.tmp" | "complete.json" | "complete.tmp" => {
                set.join(artifact)
            }
            _ => set.join("p00000001").join(artifact),
        };
        if artifact == "partition-dir" {
            let moved = root.join("held-partition");
            fs::rename(set.join("p00000001"), &moved).unwrap();
            symlink(&moved, set.join("p00000001")).unwrap();
        } else {
            fs::remove_file(&path).unwrap();
            symlink(&target, &path).unwrap();
        }
        assert!(
            matches!(
                super::recording::inspect(&root, "r", &Limits::default()),
                Err(crate::Error::Corrupt(_))
            ),
            "symlink {artifact}"
        );
        fs::remove_dir_all(root).unwrap();
    }
    let root = temp_root();
    let mut w = writer(&root, Limits::default());
    w.append(&event(1)).unwrap();
    w.finish().unwrap();
    let alias = root.join("root-alias");
    symlink(&root, &alias).unwrap();
    assert!(super::recording::inspect(&alias, "r", &Limits::default()).is_err());
    fs::remove_file(alias).unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn unexpected_root_entries_and_partition_holes_fail_closed() {
    let root = temp_root();
    let mut w = writer(&root, Limits::default());
    w.append(&event(1)).unwrap();
    w.finish().unwrap();
    fs::write(root.join("r/unexpected"), b"x").unwrap();
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    fs::remove_file(root.join("r/unexpected")).unwrap();
    fs::rename(root.join("r/p00000001"), root.join("r/p00000002")).unwrap();
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    fs::remove_dir_all(root).unwrap();

    let root = temp_root();
    let mut l = Limits::default();
    l.partition.max_partition_bytes = event_size();
    let mut w = writer(&root, l);
    for seq in 1..=3 {
        w.append(&event(seq)).unwrap();
    }
    w.finish().unwrap();
    fs::remove_dir_all(root.join("r/p00000002")).unwrap();
    let before = artifact_snapshot(&root.join("r"));
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    assert_eq!(before, artifact_snapshot(&root.join("r")));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn committed_corruption_at_first_middle_last_partition_never_returns_a_prefix() {
    for position in 0..3 {
        for field in [
            "payload_checksum",
            "schema_version",
            "collector_id",
            "session_id",
            "collector_region",
            "ingest_sequence",
            "monotonic_elapsed_nanos",
        ] {
            let root = temp_root();
            let mut limits = Limits::default();
            limits.partition.max_partition_bytes = event_size();
            limits.max_partitions = 4;
            let mut writer = writer(&root, limits.clone());
            for seq in 1..=4 {
                writer.append(&event(seq)).unwrap();
            }
            drop(writer);
            let dir = root.join("r");
            assert!(!dir.join("complete.json").exists());
            let baseline = super::recording::inspect(&root, "r", &limits).unwrap();
            assert_eq!(baseline.recording_id, "r");
            assert_eq!(
                baseline.scope,
                Scope {
                    collector_id: "c".into(),
                    collector_region: "unknown".into(),
                    session_id: "sess".into()
                }
            );
            let original_manifests = match baseline.status {
                super::recording::Status::Interrupted {
                    committed,
                    uncommitted_partition,
                } => {
                    assert_eq!(committed.len(), 3);
                    assert_eq!(
                        committed
                            .iter()
                            .map(|m| m.partition_id.as_str())
                            .collect::<Vec<_>>(),
                        vec!["p00000001", "p00000002", "p00000003"]
                    );
                    assert_eq!(uncommitted_partition.as_deref(), Some("p00000004"));
                    committed
                }
                _ => panic!("no-completion fixture is interrupted"),
            };
            for manifest in &original_manifests {
                assert_eq!(
                    crate::load(&dir, &manifest.partition_id, &Limits::default().partition)
                        .unwrap()
                        .0,
                    *manifest
                );
            }

            let id = original_manifests[position].partition_id.clone();
            let event_path = dir.join(&id).join("events.jsonl");
            let original = fs::read(&event_path).unwrap();
            let mut value: serde_json::Value = serde_json::from_slice(&original).unwrap();
            match field {
                "payload_checksum" => value["raw_payload"][0] = serde_json::json!(2),
                "schema_version" => value["schema_version"] = serde_json::json!(99),
                "collector_id" => value["collector_id"] = serde_json::json!("other"),
                "session_id" => value["collector_session_id"] = serde_json::json!("other"),
                "collector_region" => value["collector_region"] = serde_json::json!("other"),
                "ingest_sequence" => value["ingest_sequence"] = serde_json::json!(10),
                "monotonic_elapsed_nanos" if position == 0 => {
                    value["monotonic_elapsed_nanos"] = serde_json::json!(10)
                }
                _ => value["monotonic_elapsed_nanos"] = serde_json::json!(0),
            }
            let mut data = serde_json::to_vec(&value).unwrap();
            data.push(b'\n');
            fs::write(&event_path, &data).unwrap();
            let manifest_path = dir.join(&id).join("manifest.json");
            let mut manifest: serde_json::Value =
                serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
            manifest["event_bytes"] = serde_json::json!(u64::try_from(data.len()).unwrap());
            manifest["sha256"] = serde_json::json!(hex::encode(Sha256::digest(&data)));
            if field == "collector_id" {
                manifest["collector_id"] = serde_json::json!("other");
            }
            if field == "session_id" {
                manifest["collector_session_id"] = serde_json::json!("other");
            }
            if field == "ingest_sequence" {
                manifest["first_ingest_sequence"] = serde_json::json!(10);
                manifest["last_ingest_sequence"] = serde_json::json!(10);
            }
            fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

            let raw_result = crate::load(&dir, &id, &Limits::default().partition);
            if field == "payload_checksum" || field == "schema_version" {
                assert!(raw_result.is_err(), "raw load rejects {field} at {id}");
            } else {
                let updated_manifest: crate::Manifest = serde_json::from_value(manifest).unwrap();
                assert_eq!(
                    raw_result.unwrap().0,
                    updated_manifest,
                    "set-level mutation remains raw-partition valid: {field} at {id}"
                );
            }
            assert!(!dir.join("complete.json").exists());
            let before = artifact_snapshot(&dir);
            assert!(
                matches!(
                    super::recording::inspect(&root, "r", &Limits::default()),
                    Err(crate::Error::Corrupt(_))
                ),
                "recording inspect rejects {field} at {id} after valid interrupted baseline"
            );
            assert_eq!(before, artifact_snapshot(&dir));
            fs::remove_dir_all(root).unwrap();
        }
    }
}

#[test]
fn first_partition_monotonic_increase_is_raw_valid_but_breaks_next_boundary() {
    let root = temp_root();
    let mut limits = Limits::default();
    limits.partition.max_records = 2;
    limits.max_partitions = 3;
    let mut writer = writer(&root, limits.clone());
    for seq in 1..=5 {
        writer.append(&event(seq)).unwrap();
    }
    drop(writer);
    let dir = root.join("r");
    assert!(!dir.join("complete.json").exists());
    let baseline = super::recording::inspect(&root, "r", &limits).unwrap();
    let original_manifests = match baseline.status {
        super::recording::Status::Interrupted {
            committed,
            uncommitted_partition,
        } => {
            assert_eq!(committed.len(), 2);
            assert_eq!(committed[0].partition_id, "p00000001");
            assert_eq!(committed[1].partition_id, "p00000002");
            assert_eq!(uncommitted_partition.as_deref(), Some("p00000003"));
            committed
        }
        _ => panic!("no-completion control is interrupted"),
    };
    for manifest in &original_manifests {
        assert_eq!(
            crate::load(&dir, &manifest.partition_id, &Limits::default().partition)
                .unwrap()
                .0,
            *manifest
        );
    }

    let path = dir.join("p00000001/events.jsonl");
    let raw = fs::read(&path).unwrap();
    let mut lines = raw
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    lines[1]["monotonic_elapsed_nanos"] = serde_json::json!(10);
    let mut data = Vec::new();
    for line in &lines {
        data.extend(serde_json::to_vec(line).unwrap());
        data.push(b'\n');
    }
    fs::write(&path, &data).unwrap();
    let manifest_path = dir.join("p00000001/manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["event_bytes"] = serde_json::json!(u64::try_from(data.len()).unwrap());
    manifest["sha256"] = serde_json::json!(hex::encode(Sha256::digest(&data)));
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

    let p1_events = crate::load(&dir, "p00000001", &Limits::default().partition)
        .unwrap()
        .1;
    let p2_events = crate::load(&dir, "p00000002", &Limits::default().partition)
        .unwrap()
        .1;
    assert_eq!(p1_events[0].monotonic_elapsed_nanos, 1);
    assert_eq!(p1_events[1].monotonic_elapsed_nanos, 10);
    assert_eq!(p2_events[0].monotonic_elapsed_nanos, 3);
    assert!(!dir.join("complete.json").exists());
    let before = artifact_snapshot(&dir);
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    assert_eq!(before, artifact_snapshot(&dir));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn completion_and_descriptor_corruption_fail_closed_and_inspection_is_read_only() {
    let root = temp_root();
    let mut lim = Limits::default();
    lim.partition.max_partition_bytes = event_size();
    lim.max_partitions = 3;
    let mut w = writer(&root, lim);
    for seq in 1..=3 {
        w.append(&event(seq)).unwrap();
    }
    w.finish().unwrap();
    let dir = root.join("r");
    let before = artifact_snapshot(&dir);
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Ok(super::recording::Report {
            status: super::recording::Status::Complete(_),
            ..
        })
    ));
    assert_eq!(before, artifact_snapshot(&dir));
    let end_path = dir.join("complete.json");
    let original = fs::read(&end_path).unwrap();
    for field in [
        "records",
        "event_bytes",
        "start_sha256",
        "version",
        "partition_id",
        "sha256",
        "scope",
        "id",
        "first_seq",
        "last_seq",
        "ref_count",
        "ref_bytes",
        "min_time",
        "max_time",
        "order",
        "missing_ref",
        "duplicate_ref",
    ] {
        let mut value: serde_json::Value = serde_json::from_slice(&original).unwrap();
        match field {
            "records" => value["records"] = serde_json::json!(99),
            "event_bytes" => value["event_bytes"] = serde_json::json!(99),
            "start_sha256" => value["start_sha256"] = serde_json::json!("0".repeat(64)),
            "version" => value["version"] = serde_json::json!(99),
            "id" => value["id"] = serde_json::json!("wrong"),
            "scope" => value["scope"]["collector_region"] = serde_json::json!("wrong"),
            "partition_id" => {
                value["partitions"][0]["partition_id"] = serde_json::json!("p00000002")
            }
            "first_seq" => value["partitions"][0]["first_ingest_sequence"] = serde_json::json!(2),
            "last_seq" => value["partitions"][0]["last_ingest_sequence"] = serde_json::json!(2),
            "ref_count" => value["partitions"][0]["record_count"] = serde_json::json!(99),
            "ref_bytes" => value["partitions"][0]["event_bytes"] = serde_json::json!(99),
            "min_time" => {
                value["partitions"][0]["min_available_at"] =
                    serde_json::json!("2020-01-01T00:00:00Z")
            }
            "max_time" => {
                value["partitions"][0]["max_available_at"] =
                    serde_json::json!("2020-01-01T00:00:00Z")
            }
            "order" => value["partitions"].as_array_mut().unwrap().reverse(),
            "missing_ref" => {
                value["partitions"].as_array_mut().unwrap().remove(1);
            }
            "duplicate_ref" => {
                let first = value["partitions"][0].clone();
                value["partitions"][1] = first;
            }
            _ => value["partitions"][0]["sha256"] = serde_json::json!("0".repeat(64)),
        }
        fs::write(&end_path, serde_json::to_vec(&value).unwrap()).unwrap();
        let before_error = artifact_snapshot(&dir);
        assert!(
            matches!(
                super::recording::inspect(&root, "r", &Limits::default()),
                Err(crate::Error::Corrupt(_))
            ),
            "forged completion {field}"
        );
        assert_eq!(before_error, artifact_snapshot(&dir));
    }
    fs::write(&end_path, &original).unwrap();
    let events_path = dir.join("p00000001/events.jsonl");
    let events = fs::read(&events_path).unwrap();
    fs::remove_file(&events_path).unwrap();
    let before_missing_data = artifact_snapshot(&dir);
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    assert_eq!(before_missing_data, artifact_snapshot(&dir));
    fs::write(&events_path, events).unwrap();
    let start_path = dir.join("recording.json");
    let start = fs::read(&start_path).unwrap();
    let mut unknown: serde_json::Value = serde_json::from_slice(&start).unwrap();
    unknown["version"] = serde_json::json!(9);
    fs::write(&start_path, serde_json::to_vec(&unknown).unwrap()).unwrap();
    let before_bad_version = artifact_snapshot(&dir);
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    assert_eq!(before_bad_version, artifact_snapshot(&dir));
    fs::write(&start_path, &start).unwrap();
    let text = std::str::from_utf8(&start).unwrap();
    let duplicate = format!("{{\"version\":1,{}", text.get(1..).unwrap());
    fs::write(&start_path, duplicate).unwrap();
    let before_duplicate = artifact_snapshot(&dir);
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    assert_eq!(before_duplicate, artifact_snapshot(&dir));
    fs::write(&start_path, start).unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn descriptor_completion_missing_duplicate_and_unknown_fields_fail_closed() {
    let root = temp_root();
    let mut w = writer(&root, Limits::default());
    w.append(&event(1)).unwrap();
    w.finish().unwrap();
    let dir = root.join("r");
    let start_path = dir.join("recording.json");
    let start = fs::read(&start_path).unwrap();
    for field in [
        "version",
        "id",
        "scope",
        "scope.collector_id",
        "scope.collector_region",
        "scope.session_id",
    ] {
        let mut v: serde_json::Value = serde_json::from_slice(&start).unwrap();
        if field.starts_with("scope.") {
            v["scope"]
                .as_object_mut()
                .unwrap()
                .remove(field.trim_start_matches("scope."));
        } else {
            v.as_object_mut().unwrap().remove(field);
        }
        fs::write(&start_path, serde_json::to_vec(&v).unwrap()).unwrap();
        let before = artifact_snapshot(&dir);
        assert!(
            matches!(
                super::recording::inspect(&root, "r", &Limits::default()),
                Err(crate::Error::Corrupt(_))
            ),
            "start missing {field}"
        );
        assert_eq!(before, artifact_snapshot(&dir));
    }
    let mut v: serde_json::Value = serde_json::from_slice(&start).unwrap();
    v["version"] = serde_json::json!(99);
    fs::write(&start_path, serde_json::to_vec(&v).unwrap()).unwrap();
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    fs::write(&start_path, &start).unwrap();
    let mut text = String::from_utf8(start.clone()).unwrap();
    text = text.replacen(
        "\"collector_id\":\"c\"",
        "\"collector_id\":\"c\",\"collector_id\":\"c\"",
        1,
    );
    fs::write(&start_path, text).unwrap();
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    fs::write(&start_path, &start).unwrap();
    let end_path = dir.join("complete.json");
    let end = fs::read(&end_path).unwrap();
    for field in [
        "version",
        "start_sha256",
        "id",
        "scope",
        "partitions",
        "event_bytes",
        "records",
        "scope.collector_id",
        "scope.collector_region",
        "scope.session_id",
    ] {
        let mut v: serde_json::Value = serde_json::from_slice(&end).unwrap();
        if field.starts_with("scope.") {
            v["scope"]
                .as_object_mut()
                .unwrap()
                .remove(field.trim_start_matches("scope."));
        } else {
            v.as_object_mut().unwrap().remove(field);
        }
        fs::write(&end_path, serde_json::to_vec(&v).unwrap()).unwrap();
        let before = artifact_snapshot(&dir);
        assert!(
            matches!(
                super::recording::inspect(&root, "r", &Limits::default()),
                Err(crate::Error::Corrupt(_))
            ),
            "completion missing {field}"
        );
        assert_eq!(before, artifact_snapshot(&dir));
    }
    let mut v: serde_json::Value = serde_json::from_slice(&end).unwrap();
    v["version"] = serde_json::json!(99);
    fs::write(&end_path, serde_json::to_vec(&v).unwrap()).unwrap();
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    fs::write(&end_path, &end).unwrap();
    let text = String::from_utf8(end.clone()).unwrap();
    let duplicate = text.replacen("\"records\":1", "\"records\":1,\"records\":1", 1);
    fs::write(&end_path, duplicate).unwrap();
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    fs::write(&end_path, &end).unwrap();
    let text = String::from_utf8(end.clone()).unwrap();
    let duplicate = text.replacen(
        "\"collector_id\":\"c\"",
        "\"collector_id\":\"c\",\"collector_id\":\"c\"",
        1,
    );
    fs::write(&end_path, duplicate).unwrap();
    assert!(matches!(
        super::recording::inspect(&root, "r", &Limits::default()),
        Err(crate::Error::Corrupt(_))
    ));
    fs::write(&end_path, end).unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn completion_faults_poison_and_postpublish_sync_failure_is_readable_complete() {
    use super::recording::SetFault;
    for point in [
        SetFault::Write,
        SetFault::Sync,
        SetFault::Publish,
        SetFault::DirectorySync,
        SetFault::ParentSync,
    ] {
        let root = temp_root();
        let mut w = writer(&root, Limits::default());
        w.append(&event(1)).unwrap();
        w.inject_finish_fault(point);
        assert!(matches!(w.finish(), Err(crate::Error::Io(_))));
        assert!(matches!(w.finish(), Err(crate::Error::Poisoned)));
        assert!(matches!(w.append(&event(2)), Err(crate::Error::Poisoned)));
        let expected_manifest =
            crate::load(root.join("r"), "p00000001", &Limits::default().partition)
                .unwrap()
                .0;
        let partition_dir = root.join("r/p00000001");
        assert!(partition_dir.join("manifest.json").exists());
        assert!(partition_dir.join("manifest.tmp").exists());
        assert_eq!(
            fs::read(partition_dir.join("manifest.json")).unwrap(),
            fs::read(partition_dir.join("manifest.tmp")).unwrap()
        );
        let start_sha = hex::encode(Sha256::digest(
            fs::read(root.join("r/recording.json")).unwrap(),
        ));
        drop(w);
        let final_exists = root.join("r/complete.json").exists();
        assert_eq!(
            final_exists,
            matches!(point, SetFault::DirectorySync | SetFault::ParentSync)
        );
        let temp = root.join("r/complete.tmp");
        assert!(temp.exists());
        if point == SetFault::Write {
            assert_eq!(fs::metadata(&temp).unwrap().len(), 0);
        } else {
            assert!(fs::metadata(&temp).unwrap().len() > 0);
        }
        if final_exists {
            assert_eq!(
                fs::read(&temp).unwrap(),
                fs::read(root.join("r/complete.json")).unwrap()
            );
        }
        let before = artifact_snapshot(&root.join("r"));
        let report = super::recording::inspect(&root, "r", &Limits::default()).unwrap();
        assert_eq!(before, artifact_snapshot(&root.join("r")));
        assert_eq!(report.start_sha256, start_sha);
        match (point, report.status) {
            (
                SetFault::DirectorySync | SetFault::ParentSync,
                super::recording::Status::Complete(receipt),
            ) => {
                assert_eq!(receipt.recording_id, "r");
                assert_eq!(
                    receipt.scope,
                    Scope {
                        collector_id: "c".into(),
                        collector_region: "unknown".into(),
                        session_id: "sess".into()
                    }
                );
                assert_eq!(receipt.start_sha256, start_sha);
                assert_eq!(receipt.partitions, vec![expected_manifest.clone()]);
                assert_eq!(receipt.event_bytes, expected_manifest.event_bytes);
                assert_eq!(receipt.records, expected_manifest.record_count);
            }
            (
                _,
                super::recording::Status::Interrupted {
                    committed,
                    uncommitted_partition,
                },
            ) => {
                assert_eq!(committed, vec![expected_manifest.clone()]);
                assert_eq!(uncommitted_partition, None);
            }
            _ => panic!("completion fault recovery status"),
        }
        fs::remove_dir_all(root).unwrap();
    }
}
