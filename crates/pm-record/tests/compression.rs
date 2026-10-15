#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use chrono::{TimeZone, Utc};
use flate2::{Compression, write::GzEncoder};
use pm_record::{
    Error, Inspection, Limits, Writer, compression, inspect, load,
    recording::{self, Scope, Status},
};
use pm_schema::{ClockStatus, EventEnvelope, EventKind, sha256};
use std::{
    fs,
    io::{Read, Write},
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

static NEXT: AtomicUsize = AtomicUsize::new(0);

fn root() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "pm-record-compression-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&path).expect("temporary root");
    path
}

fn event(sequence: u64, payload: Vec<u8>) -> EventEnvelope {
    let time = Utc
        .timestamp_opt(1_800_000_000, 0)
        .single()
        .expect("timestamp");
    EventEnvelope {
        schema_version: 1,
        source: "synthetic".into(),
        stream: "stream".into(),
        collector_id: "collector".into(),
        collector_region: "test-region".into(),
        connection_id: "connection".into(),
        collector_session_id: "session".into(),
        ingest_sequence: sequence,
        source_sequence: None,
        source_event_time: None,
        source_publish_time: None,
        received_at: time,
        available_at: time,
        monotonic_elapsed_nanos: sequence,
        provider_time_unit: None,
        kind: EventKind::Gap,
        raw_payload: payload.clone(),
        quality_flags: vec!["SYNTHETIC_TEST".into()],
        payload_sha256: sha256(&payload),
        clock_status: ClockStatus::Unknown,
    }
}

fn committed(root: &PathBuf, id: &str) -> Vec<u8> {
    let mut writer = Writer::create(root, id, Limits::default()).expect("writer");
    writer
        .append(&event(1, vec![b'x'; 10_000]))
        .expect("append");
    writer.seal().expect("seal");
    fs::read(root.join(id).join("events.jsonl")).expect("raw JSONL")
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(1));
    encoder.write_all(bytes).expect("compress input");
    encoder.finish().expect("finish gzip")
}

#[test]
fn pack_preserves_manifest_and_loads_compressed_partition_idempotently() {
    let root = root();
    let raw = committed(&root, "packed");
    let manifest_before = load(&root, "packed", &Limits::default()).unwrap().0;
    let manifest_bytes_before = fs::read(root.join("packed/manifest.json")).unwrap();

    let report = compression::pack(&root, "packed", &Limits::default()).unwrap();
    assert_eq!(report.manifest, manifest_before);
    assert_eq!(
        report.manifest.event_bytes,
        u64::try_from(raw.len()).unwrap()
    );
    assert_eq!(
        report.compressed_bytes,
        fs::metadata(root.join("packed/events.jsonl.gz"))
            .unwrap()
            .len()
    );
    assert_eq!(
        fs::read(root.join("packed/manifest.json")).unwrap(),
        manifest_bytes_before
    );
    let compressed = fs::read(root.join("packed/events.jsonl.gz")).unwrap();
    let mut decoder = flate2::read::GzDecoder::new(compressed.as_slice());
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded).unwrap();
    assert_eq!(decoded, raw);
    assert!(report.compressed_bytes < report.manifest.event_bytes);
    assert!(!root.join("packed/events.jsonl").exists());
    assert!(!root.join("packed/events.jsonl.gz.tmp").exists());

    let (manifest_after, records) = load(&root, "packed", &Limits::default()).unwrap();
    assert_eq!(manifest_after, manifest_before);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].raw_payload, vec![b'x'; 10_000]);
    assert_eq!(records[0].ingest_sequence, 1);
    assert!(
        matches!(inspect(&root, "packed", &Limits::default()).unwrap(), Inspection::Committed(m) if m == manifest_before)
    );

    let second = compression::pack(&root, "packed", &Limits::default()).unwrap();
    assert_eq!(second.compressed_bytes, report.compressed_bytes);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn pack_completes_matching_raw_and_gzip_crash_state() {
    let root = root();
    let raw = committed(&root, "recover");
    fs::write(root.join("recover/events.jsonl.gz"), gzip(&raw)).unwrap();
    let (manifest, _) = load(&root, "recover", &Limits::default()).unwrap();
    let report = compression::pack(&root, "recover", &Limits::default()).unwrap();
    assert_eq!(report.manifest, manifest);
    assert!(!root.join("recover/events.jsonl").exists());
    assert!(root.join("recover/events.jsonl.gz").exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn mismatched_dual_state_fails_without_deleting_raw() {
    let root = root();
    let raw = committed(&root, "mismatch");
    fs::write(
        root.join("mismatch/events.jsonl.gz"),
        gzip(b"different JSONL\n"),
    )
    .unwrap();
    assert!(matches!(
        load(&root, "mismatch", &Limits::default()),
        Err(Error::Corrupt(_))
    ));
    assert!(matches!(
        compression::pack(&root, "mismatch", &Limits::default()),
        Err(Error::Corrupt(_))
    ));
    assert_eq!(fs::read(root.join("mismatch/events.jsonl")).unwrap(), raw);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn corrupt_raw_does_not_fall_back_to_matching_gzip() {
    let root = root();
    let raw = committed(&root, "raw-corrupt");
    fs::write(root.join("raw-corrupt/events.jsonl.gz"), gzip(&raw)).unwrap();
    let mut corrupt = raw.clone();
    corrupt[0] ^= 1;
    fs::write(root.join("raw-corrupt/events.jsonl"), &corrupt).unwrap();
    assert!(matches!(
        load(&root, "raw-corrupt", &Limits::default()),
        Err(Error::Corrupt(_))
    ));
    assert!(matches!(
        compression::pack(&root, "raw-corrupt", &Limits::default()),
        Err(Error::Corrupt(_))
    ));
    assert_eq!(
        fs::read(root.join("raw-corrupt/events.jsonl")).unwrap(),
        corrupt
    );
    fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn matching_published_temp_hardlink_finishes_crash_state() {
    let root = root();
    let raw = committed(&root, "link-recover");
    let gzip_path = root.join("link-recover/events.jsonl.gz");
    let temp_path = root.join("link-recover/events.jsonl.gz.tmp");
    fs::write(&gzip_path, gzip(&raw)).unwrap();
    fs::hard_link(&gzip_path, &temp_path).unwrap();
    let report = compression::pack(&root, "link-recover", &Limits::default()).unwrap();
    assert!(!root.join("link-recover/events.jsonl").exists());
    assert!(!temp_path.exists());
    assert!(report.compressed_bytes > 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn gzip_rejects_truncation_crc_trailing_bytes_and_concatenated_members() {
    for variant in 0..4 {
        let root = root();
        let raw = committed(&root, "corrupt");
        let mut compressed = gzip(&raw);
        match variant {
            0 => {
                compressed.truncate(compressed.len() - 4);
            }
            1 => {
                let crc_index = compressed.len() - 8;
                compressed[crc_index] ^= 1;
            }
            2 => compressed.push(0),
            _ => compressed.extend(gzip(b"extra member")),
        }
        fs::remove_file(root.join("corrupt/events.jsonl")).unwrap();
        fs::write(root.join("corrupt/events.jsonl.gz"), compressed).unwrap();
        assert!(matches!(
            load(&root, "corrupt", &Limits::default()),
            Err(Error::Corrupt(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn decompressed_record_bound_rejects_gzip_expansion() {
    let root = root();
    let raw = committed(&root, "expanded");
    let manifest = load(&root, "expanded", &Limits::default()).unwrap().0;
    fs::remove_file(root.join("expanded/events.jsonl")).unwrap();
    let limits = Limits {
        max_record_bytes: manifest.event_bytes,
        ..Limits::default()
    };
    let mut expansion = raw;
    expansion.extend(vec![b'a'; 100_000]);
    fs::write(root.join("expanded/events.jsonl.gz"), gzip(&expansion)).unwrap();
    assert!(matches!(
        load(&root, "expanded", &limits),
        Err(Error::Corrupt(_))
    ));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn small_manifest_with_large_record_gzip_bomb_is_capped_by_logical_bytes() {
    let root = root();
    let mut writer = Writer::create(&root, "zipbomb", Limits::default()).unwrap();
    writer.append(&event(1, vec![b'x'])).unwrap();
    writer.seal().unwrap();
    let raw = fs::read(root.join("zipbomb/events.jsonl")).unwrap();
    let manifest = load(&root, "zipbomb", &Limits::default()).unwrap().0;
    fs::remove_file(root.join("zipbomb/events.jsonl")).unwrap();

    let mut expansion = raw;
    expansion.extend(vec![b'a'; 8 * 1024 * 1024]);
    fs::write(root.join("zipbomb/events.jsonl.gz"), gzip(&expansion)).unwrap();
    let limits = Limits {
        max_record_bytes: 16 * 1024 * 1024,
        max_partition_bytes: manifest.event_bytes,
        ..Limits::default()
    };
    assert!(matches!(
        load(&root, "zipbomb", &limits),
        Err(Error::Corrupt(_))
    ));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn compressed_physical_file_bound_is_enforced() {
    let root = root();
    let raw = committed(&root, "physical");
    let manifest = load(&root, "physical", &Limits::default()).unwrap().0;
    fs::remove_file(root.join("physical/events.jsonl")).unwrap();
    let mut compressed = gzip(&raw);
    let physical_limit = manifest.event_bytes.checked_add(64 * 1024).unwrap();
    compressed.resize(usize::try_from(physical_limit + 1).unwrap(), 0);
    fs::write(root.join("physical/events.jsonl.gz"), compressed).unwrap();
    let limits = Limits {
        max_partition_bytes: manifest.event_bytes,
        ..Limits::default()
    };
    assert!(matches!(
        load(&root, "physical", &limits),
        Err(Error::Corrupt(_))
    ));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn stale_temp_is_preserved_and_does_not_replace_raw_load() {
    let root = root();
    let raw = committed(&root, "temp");
    let temp = root.join("temp/events.jsonl.gz.tmp");
    fs::write(&temp, b"stale temporary").unwrap();
    assert_eq!(load(&root, "temp", &Limits::default()).unwrap().1.len(), 1);
    assert!(compression::pack(&root, "temp", &Limits::default()).is_err());
    assert_eq!(fs::read(&temp).unwrap(), b"stale temporary");
    assert_eq!(fs::read(root.join("temp/events.jsonl")).unwrap(), raw);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn recording_inspector_accepts_compressed_partition_and_bounded_temp() {
    let root = root();
    let scope = Scope {
        collector_id: "collector".into(),
        collector_region: "test-region".into(),
        session_id: "session".into(),
    };
    let mut writer =
        recording::Writer::create(&root, "recording", scope, recording::Limits::default()).unwrap();
    writer.append(&event(1, vec![b'x'; 10_000])).unwrap();
    writer.finish().unwrap();
    let recording_root = root.join("recording");
    let raw = fs::read(recording_root.join("p00000001/events.jsonl")).unwrap();
    compression::pack(&recording_root, "p00000001", &Limits::default()).unwrap();
    fs::write(recording_root.join("p00000001/events.jsonl"), raw).unwrap();
    let temp = recording_root.join("p00000001/events.jsonl.gz.tmp");
    fs::hard_link(recording_root.join("p00000001/events.jsonl.gz"), &temp).unwrap();
    assert_eq!(
        fs::read(recording_root.join("p00000001/manifest.json")).unwrap(),
        fs::read(recording_root.join("p00000001/manifest.tmp")).unwrap()
    );
    let report = recording::inspect(&root, "recording", &recording::Limits::default()).unwrap();
    assert!(matches!(report.status, Status::Complete(_)));
    fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn compressed_event_symlink_is_rejected_even_when_raw_exists() {
    use std::os::unix::fs::symlink;

    let root = root();
    committed(&root, "linked");
    let outside = root.join("outside.gz");
    fs::write(&outside, gzip(b"x")).unwrap();
    symlink(&outside, root.join("linked/events.jsonl.gz")).unwrap();
    assert!(matches!(
        load(&root, "linked", &Limits::default()),
        Err(Error::Corrupt(_))
    ));
    fs::remove_dir_all(root).unwrap();
}
