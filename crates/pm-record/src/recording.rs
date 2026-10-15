//! Bounded, single-session recording sets built from the existing immutable v1 partitions.
//! A finished set records storage completion only; it is not evidence of source completeness.
use crate::{
    Error, Limits as PartitionLimits, Manifest, Receipt as PartitionReceipt,
    Writer as PartitionWriter,
};
use pm_schema::EventEnvelope;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};

const VERSION: u16 = 1;
const MAX_ORDINAL: u32 = 99_999_999;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub collector_id: String,
    pub collector_region: String,
    pub session_id: String,
}
#[derive(Clone, Debug)]
pub struct Limits {
    pub partition: PartitionLimits,
    pub max_partitions: u32,
    pub max_total_event_bytes: u64,
    pub max_total_records: u64,
    pub max_metadata_bytes: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            partition: PartitionLimits::default(),
            max_partitions: 64,
            max_total_event_bytes: 1_073_741_824,
            max_total_records: 1_000_000,
            max_metadata_bytes: 65_536,
        }
    }
}
#[derive(Debug)]
pub struct SealedPartition {
    pub ordinal: u32,
    pub receipt: PartitionReceipt,
}
#[derive(Debug)]
pub struct CompletionReceipt {
    pub recording_id: String,
    pub scope: Scope,
    pub start_sha256: String,
    pub partitions: Vec<Manifest>,
    pub event_bytes: u64,
    pub records: u64,
}
#[derive(Debug)]
pub struct Report {
    pub recording_id: String,
    pub scope: Scope,
    pub start_sha256: String,
    pub status: Status,
}
#[derive(Debug)]
pub enum Status {
    Complete(CompletionReceipt),
    Interrupted {
        committed: Vec<Manifest>,
        uncommitted_partition: Option<String>,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Start {
    version: u16,
    id: String,
    scope: Scope,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct End {
    version: u16,
    start_sha256: String,
    id: String,
    scope: Scope,
    partitions: Vec<Manifest>,
    event_bytes: u64,
    records: u64,
}

fn valid_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}
fn ordinal_id(n: u32) -> String {
    format!("p{n:08}")
}
fn next_ordinal(current: u32, max: u32) -> Result<u32, Error> {
    current
        .checked_add(1)
        .filter(|n| *n <= max && *n <= MAX_ORDINAL)
        .ok_or_else(|| Error::Limit("partition count".into()))
}
fn directory_entry_limit(max_partitions: u32) -> Result<usize, Error> {
    usize::try_from(max_partitions)
        .map_err(|_| Error::Invalid("partition limit exceeds address space".into()))?
        .checked_add(4)
        .ok_or_else(|| Error::Invalid("directory entry bound overflow".into()))
}
fn checked_limits(l: &Limits) -> Result<(), Error> {
    if l.max_partitions == 0
        || l.max_partitions > MAX_ORDINAL
        || l.max_total_event_bytes == 0
        || l.max_total_records == 0
        || l.max_metadata_bytes == 0
    {
        return Err(Error::Invalid(
            "recording limits must be positive and representable".into(),
        ));
    }
    usize::try_from(
        l.max_metadata_bytes
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("metadata bound overflow".into()))?,
    )
    .map_err(|_| Error::Invalid("metadata bound exceeds address space".into()))?;
    Ok(())
}
fn metadata<T: Serialize>(v: &T, cap: u64) -> Result<Vec<u8>, Error> {
    let cap = usize::try_from(cap)
        .map_err(|_| Error::Invalid("metadata bound exceeds address space".into()))?;
    let mut output = crate::Bounded(Vec::new(), cap);
    serde_json::to_writer(&mut output, v).map_err(|e| Error::Limit(e.to_string()))?;
    Ok(output.0)
}
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SetFault {
    Write,
    Sync,
    Publish,
    DirectorySync,
    ParentSync,
}
#[cfg(test)]
fn hit_fault(fault: &mut Option<SetFault>, point: SetFault) -> bool {
    if *fault == Some(point) {
        *fault = None;
        true
    } else {
        false
    }
}
fn publish(
    root: &Path,
    temp_name: &str,
    final_name: &str,
    bytes: &[u8],
    #[cfg(test)] fault: &mut Option<SetFault>,
) -> Result<(), Error> {
    let tmp = root.join(temp_name);
    let mut f = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
    #[cfg(test)]
    if hit_fault(fault, SetFault::Write) {
        return Err(Error::Io(std::io::Error::other(
            "injected completion write",
        )));
    }
    f.write_all(bytes)?;
    f.flush()?;
    #[cfg(test)]
    if hit_fault(fault, SetFault::Sync) {
        return Err(Error::Io(std::io::Error::other("injected completion sync")));
    }
    f.sync_all()?;
    drop(f);
    #[cfg(test)]
    if hit_fault(fault, SetFault::Publish) {
        return Err(Error::Io(std::io::Error::other(
            "injected completion publish",
        )));
    }
    fs::hard_link(&tmp, root.join(final_name))?;
    #[cfg(test)]
    if hit_fault(fault, SetFault::DirectorySync) {
        return Err(Error::Io(std::io::Error::other(
            "injected completion directory sync",
        )));
    }
    File::open(root)?.sync_all()?;
    Ok(())
}

/// A single-use synchronous recording writer. Metadata and aggregate limits are caller budgets,
/// not disk-capacity estimates; aggregate event bytes exclude metadata.
pub struct Writer {
    root: PathBuf,
    id: String,
    scope: Scope,
    start_sha256: String,
    limits: Limits,
    current: Option<PartitionWriter>,
    ordinal: u32,
    manifests: Vec<Manifest>,
    bytes: u64,
    current_bytes: u64,
    current_records: u64,
    records: u64,
    last_sequence: Option<u64>,
    last_elapsed: Option<u64>,
    max_partition_age_nanos: Option<u64>,
    current_first_elapsed: Option<u64>,
    closed: bool,
    poisoned: bool,
    #[cfg(test)]
    finish_fault: Option<SetFault>,
    #[cfg(test)]
    next_append_fault: Option<crate::FaultAt>,
}
impl Writer {
    #[cfg(test)]
    pub(crate) fn force_last_sequence_for_test(&mut self, sequence: u64) {
        self.last_sequence = Some(sequence);
    }
    #[cfg(test)]
    pub(crate) fn inject_partition_fault(&mut self, point: crate::FaultAt) {
        if let Some(current) = self.current.as_mut() {
            current.fault = Some(point);
        }
    }
    #[cfg(test)]
    pub(crate) fn inject_finish_fault(&mut self, point: SetFault) {
        self.finish_fault = Some(point);
    }
    #[cfg(test)]
    pub(crate) fn inject_next_partition_append_fault(&mut self, point: crate::FaultAt) {
        self.next_append_fault = Some(point);
    }
    pub fn create(
        root: impl AsRef<Path>,
        id: &str,
        scope: Scope,
        limits: Limits,
    ) -> Result<Self, Error> {
        Self::create_inner(root.as_ref(), id, scope, limits, None)
    }

    /// Creates a recording writer that also rotates before an event at the partition-age limit.
    ///
    /// The age is measured from the first successfully appended event in each partition using
    /// that event's `monotonic_elapsed_nanos`. No timer or empty partition is created while idle.
    ///
    /// # Errors
    ///
    /// Returns `Error::Invalid` when `max_partition_age` is zero or cannot be represented as
    /// monotonic nanoseconds in a `u64`, before creating any recording files.
    pub fn create_with_partition_age(
        root: impl AsRef<Path>,
        id: &str,
        scope: Scope,
        limits: Limits,
        max_partition_age: Duration,
    ) -> Result<Self, Error> {
        let age_nanos = max_partition_age.as_nanos();
        if age_nanos == 0 || age_nanos > u128::from(u64::MAX) {
            return Err(Error::Invalid(
                "partition age must be positive and fit monotonic nanoseconds".into(),
            ));
        }
        let age_nanos = u64::try_from(age_nanos)
            .map_err(|_| Error::Invalid("partition age exceeds monotonic range".into()))?;
        Self::create_inner(root.as_ref(), id, scope, limits, Some(age_nanos))
    }

    fn create_inner(
        root: &Path,
        id: &str,
        scope: Scope,
        limits: Limits,
        max_partition_age_nanos: Option<u64>,
    ) -> Result<Self, Error> {
        checked_limits(&limits)?;
        limits.partition.validate()?;
        if !valid_id(id)
            || !valid_id(&scope.collector_id)
            || !valid_id(&scope.session_id)
            || !valid_id(&scope.collector_region)
        {
            return Err(Error::Invalid("invalid recording identity or scope".into()));
        }
        let meta = fs::symlink_metadata(root)?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            return Err(Error::Invalid("expected trusted root directory".into()));
        }
        let start = Start {
            version: VERSION,
            id: id.into(),
            scope: scope.clone(),
        };
        let data = metadata(&start, limits.max_metadata_bytes)?;
        let dir = root.join(id);
        fs::create_dir(&dir)?;
        #[cfg(test)]
        let mut no_fault = None;
        publish(
            &dir,
            "recording.tmp",
            "recording.json",
            &data,
            #[cfg(test)]
            &mut no_fault,
        )?;
        File::open(root)?.sync_all()?;
        let start_sha256 = hex::encode(Sha256::digest(&data));
        Ok(Self {
            root: dir,
            id: id.into(),
            scope,
            start_sha256,
            limits,
            current: None,
            ordinal: 0,
            manifests: Vec::new(),
            bytes: 0,
            current_bytes: 0,
            current_records: 0,
            records: 0,
            last_sequence: None,
            last_elapsed: None,
            max_partition_age_nanos,
            current_first_elapsed: None,
            closed: false,
            poisoned: false,
            #[cfg(test)]
            finish_fault: None,
            #[cfg(test)]
            next_append_fault: None,
        })
    }
    fn check(&self) -> Result<(), Error> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else if self.closed {
            Err(Error::Sealed)
        } else {
            Ok(())
        }
    }
    pub fn append(&mut self, e: &EventEnvelope) -> Result<Option<SealedPartition>, Error> {
        self.check()?;
        e.validate().map_err(|x| Error::Invalid(x.to_string()))?;
        let expected = match self.last_sequence {
            None => 1,
            Some(n) => n
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("sequence overflow".into()))?,
        };
        if e.collector_id != self.scope.collector_id
            || e.collector_region != self.scope.collector_region
            || e.collector_session_id != self.scope.session_id
            || e.ingest_sequence != expected
            || self
                .last_elapsed
                .is_some_and(|n| e.monotonic_elapsed_nanos < n)
        {
            return Err(Error::Invalid("scope, sequence, or monotonic order".into()));
        }
        let encoded = crate::encode_event(e, &self.limits.partition)?;
        let event_bytes =
            u64::try_from(encoded.len()).map_err(|_| Error::Limit("event size".into()))?;
        let payload_bytes = u64::try_from(e.raw_payload.len())
            .map_err(|_| Error::Limit("payload size exceeds format".into()))?;
        if event_bytes > self.limits.partition.max_partition_bytes
            || payload_bytes > self.limits.partition.max_payload_bytes
        {
            return Err(Error::Limit("event cannot fit partition".into()));
        }
        if self.records >= self.limits.max_total_records
            || self
                .bytes
                .checked_add(event_bytes)
                .is_none_or(|n| n > self.limits.max_total_event_bytes)
        {
            return Err(Error::Limit("recording aggregate".into()));
        }
        let rotate = self.current.is_some()
            && (self.current_records >= self.limits.partition.max_records
                || self
                    .current_bytes
                    .checked_add(event_bytes)
                    .is_none_or(|n| n > self.limits.partition.max_partition_bytes)
                || self.max_partition_age_nanos.is_some_and(|max_age| {
                    self.current_first_elapsed.is_some_and(|first_elapsed| {
                        e.monotonic_elapsed_nanos
                            .checked_sub(first_elapsed)
                            .is_some_and(|elapsed| elapsed >= max_age)
                    })
                }));
        if rotate {
            let successor_ordinal = next_ordinal(self.ordinal, self.limits.max_partitions)?;
            let mut old = self.current.take().ok_or(Error::Poisoned)?;
            let receipt = match old.seal() {
                Ok(r) => r,
                Err(e) => {
                    self.poisoned = true;
                    return Err(e);
                }
            };
            self.manifests.push(receipt.manifest.clone());
            self.current_bytes = 0;
            self.current_records = 0;
            self.current_first_elapsed = None;
            self.ordinal = successor_ordinal;
            match PartitionWriter::create(
                &self.root,
                &ordinal_id(self.ordinal),
                self.limits.partition.clone(),
            ) {
                Ok(w) => {
                    #[cfg(test)]
                    let w = {
                        let mut w = w;
                        if let Some(point) = self.next_append_fault.take() {
                            w.fault = Some(point);
                        }
                        w
                    };
                    self.current = Some(w)
                }
                Err(e) => {
                    self.poisoned = true;
                    return Err(e);
                }
            }
            let w = self.current.as_mut().ok_or(Error::Poisoned)?;
            if let Err(error) = w.append_encoded(e, encoded) {
                self.poisoned = true;
                return Err(error);
            }
            self.current_bytes = event_bytes;
            self.current_records = 1;
            self.bytes = self
                .bytes
                .checked_add(event_bytes)
                .ok_or_else(|| Error::Limit("recording byte overflow".into()))?;
            self.records = self
                .records
                .checked_add(1)
                .ok_or_else(|| Error::Limit("recording record overflow".into()))?;
            self.current_first_elapsed = Some(e.monotonic_elapsed_nanos);
            self.last_sequence = Some(e.ingest_sequence);
            self.last_elapsed = Some(e.monotonic_elapsed_nanos);
            return Ok(Some(SealedPartition {
                ordinal: self
                    .ordinal
                    .checked_sub(1)
                    .ok_or_else(|| Error::Invalid("rotation ordinal underflow".into()))?,
                receipt,
            }));
        }
        if self.current.is_none() {
            self.ordinal = next_ordinal(self.ordinal, self.limits.max_partitions)?;
            self.current = match PartitionWriter::create(
                &self.root,
                &ordinal_id(self.ordinal),
                self.limits.partition.clone(),
            ) {
                Ok(w) => Some(w),
                Err(e) => {
                    self.poisoned = true;
                    return Err(e);
                }
            };
        }
        let w = self.current.as_mut().ok_or(Error::Poisoned)?;
        if let Err(error) = w.append_encoded(e, encoded) {
            if matches!(error, Error::Io(_)) {
                self.poisoned = true;
            }
            return Err(error);
        }
        if self.max_partition_age_nanos.is_some() && self.current_records == 0 {
            self.current_first_elapsed = Some(e.monotonic_elapsed_nanos);
        }
        self.current_bytes = self
            .current_bytes
            .checked_add(event_bytes)
            .ok_or_else(|| Error::Limit("partition byte overflow".into()))?;
        self.current_records = self
            .current_records
            .checked_add(1)
            .ok_or_else(|| Error::Limit("partition record overflow".into()))?;
        self.bytes = self
            .bytes
            .checked_add(event_bytes)
            .ok_or_else(|| Error::Limit("recording byte overflow".into()))?;
        self.records = self
            .records
            .checked_add(1)
            .ok_or_else(|| Error::Limit("recording record overflow".into()))?;
        self.last_sequence = Some(e.ingest_sequence);
        self.last_elapsed = Some(e.monotonic_elapsed_nanos);
        Ok(None)
    }
    pub fn finish(&mut self) -> Result<CompletionReceipt, Error> {
        self.check()?;
        if self.records == 0 {
            return Err(Error::Invalid("empty recording".into()));
        }
        if let Some(mut w) = self.current.take() {
            let r = match w.seal() {
                Ok(r) => r,
                Err(e) => {
                    self.poisoned = true;
                    return Err(e);
                }
            };
            self.manifests.push(r.manifest);
        }
        let end = End {
            version: VERSION,
            start_sha256: self.start_sha256.clone(),
            id: self.id.clone(),
            scope: self.scope.clone(),
            partitions: self.manifests.clone(),
            event_bytes: self.bytes,
            records: self.records,
        };
        let data = match metadata(&end, self.limits.max_metadata_bytes) {
            Ok(v) => v,
            Err(e) => {
                self.poisoned = true;
                return Err(e);
            }
        };
        if let Err(e) = publish(
            &self.root,
            "complete.tmp",
            "complete.json",
            &data,
            #[cfg(test)]
            &mut self.finish_fault,
        ) {
            self.poisoned = true;
            return Err(e);
        }
        let parent = self.root.parent().ok_or_else(|| {
            self.poisoned = true;
            Error::Invalid("recording root has no parent".into())
        })?;
        #[cfg(test)]
        if hit_fault(&mut self.finish_fault, SetFault::ParentSync) {
            self.poisoned = true;
            return Err(Error::Io(std::io::Error::other(
                "injected completion parent sync",
            )));
        }
        let sync = File::open(parent).and_then(|f| f.sync_all());
        if let Err(e) = sync {
            self.poisoned = true;
            return Err(Error::Io(e));
        }
        self.closed = true;
        Ok(CompletionReceipt {
            recording_id: self.id.clone(),
            scope: self.scope.clone(),
            start_sha256: self.start_sha256.clone(),
            partitions: self.manifests.clone(),
            event_bytes: self.bytes,
            records: self.records,
        })
    }
}

/// Read-only inspection. The caller must inspect a quiescent, trusted local filesystem.
pub fn inspect(root: impl AsRef<Path>, id: &str, limits: &Limits) -> Result<Report, Error> {
    checked_limits(limits)?;
    limits.partition.validate()?;
    if !valid_id(id) {
        return Err(Error::Invalid("invalid recording id".into()));
    }
    let root = root.as_ref();
    let root_meta = fs::symlink_metadata(root)?;
    if !root_meta.is_dir() || root_meta.file_type().is_symlink() {
        return Err(Error::Invalid("expected trusted root directory".into()));
    }
    let dir = root.join(id);
    let dir_meta = match fs::symlink_metadata(&dir) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(Error::Uncommitted),
        Err(e) => return Err(e.into()),
    };
    if !dir_meta.is_dir() || dir_meta.file_type().is_symlink() {
        return Err(Error::Corrupt("recording directory is not regular".into()));
    }

    let start_path = dir.join("recording.json");
    let start_bytes =
        crate::bounded_file(&start_path, limits.max_metadata_bytes).map_err(|e| match e {
            Error::Io(io) if io.kind() == std::io::ErrorKind::NotFound => Error::Uncommitted,
            x => x,
        })?;
    let start: Start =
        serde_json::from_slice(&start_bytes).map_err(|e| Error::Corrupt(e.to_string()))?;
    if start.version != VERSION
        || start.id != id
        || !valid_id(&start.scope.collector_id)
        || !valid_id(&start.scope.session_id)
        || !valid_id(&start.scope.collector_region)
    {
        return Err(Error::Corrupt(
            "start descriptor fields inconsistent".into(),
        ));
    }
    let start_sha = hex::encode(Sha256::digest(&start_bytes));

    // Read no more than the caller's partition allowance plus the four defined metadata names.
    let mut root_names = Vec::new();
    let entry_limit = directory_entry_limit(limits.max_partitions)?;
    for entry in fs::read_dir(&dir)? {
        if root_names.len() >= entry_limit {
            return Err(Error::Limit("recording directory entries".into()));
        }
        let entry = entry?;
        let entry_meta = fs::symlink_metadata(entry.path())?;
        if entry_meta.file_type().is_symlink() {
            return Err(Error::Corrupt("symlink recording artifact".into()));
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| Error::Corrupt("non-UTF8 recording entry".into()))?;
        root_names.push(name);
    }
    let allowed = [
        "recording.json",
        "recording.tmp",
        "complete.json",
        "complete.tmp",
    ];
    let mut ordinals = Vec::new();
    for name in &root_names {
        let entry_meta = fs::symlink_metadata(dir.join(name))?;
        if entry_meta.file_type().is_symlink() {
            return Err(Error::Corrupt("symlink recording artifact".into()));
        }
        if allowed.contains(&name.as_str()) {
            if !entry_meta.is_file() || entry_meta.len() > limits.max_metadata_bytes {
                return Err(Error::Corrupt(
                    "invalid or oversized recording metadata artifact".into(),
                ));
            }
            continue;
        }
        if name.len() == 9 && name.starts_with('p') && name[1..].bytes().all(|b| b.is_ascii_digit())
        {
            let n: u32 = name[1..]
                .parse()
                .map_err(|_| Error::Corrupt("partition ordinal overflow".into()))?;
            if n == 0 || n > MAX_ORDINAL || ordinal_id(n) != *name {
                return Err(Error::Corrupt("noncanonical partition ordinal".into()));
            }
            if !entry_meta.is_dir() {
                return Err(Error::Corrupt("partition entry is not a directory".into()));
            }
            ordinals.push(n);
        } else {
            return Err(Error::Corrupt(format!("unknown recording entry {name}")));
        }
    }
    ordinals.sort_unstable();
    let partition_limit = usize::try_from(limits.max_partitions)
        .map_err(|_| Error::Invalid("partition limit exceeds address space".into()))?;
    if ordinals.len() > partition_limit {
        return Err(Error::Limit("partition count".into()));
    }
    if ordinals
        .iter()
        .enumerate()
        .any(|(i, n)| usize::try_from(*n).ok() != Some(i + 1))
    {
        return Err(Error::Corrupt("partition ordinal hole".into()));
    }

    // Both sides of hard-link publication are expected names; reject symlink/unknown entries.
    for ordinal in &ordinals {
        let p = dir.join(ordinal_id(*ordinal));
        let pm = fs::symlink_metadata(&p)?;
        if !pm.is_dir() || pm.file_type().is_symlink() {
            return Err(Error::Corrupt(
                "partition is not a regular directory".into(),
            ));
        }
        let mut names = Vec::new();
        for entry in fs::read_dir(&p)? {
            if names.len() >= 5 {
                return Err(Error::Corrupt("unexpected partition entry count".into()));
            }
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| Error::Corrupt("non-UTF8 partition entry".into()))?;
            if ![
                "events.jsonl",
                "events.jsonl.gz",
                "events.jsonl.gz.tmp",
                "manifest.json",
                "manifest.tmp",
            ]
            .contains(&name.as_str())
            {
                return Err(Error::Corrupt(format!("unknown partition entry {name}")));
            }
            let m = fs::symlink_metadata(entry.path())?;
            if m.file_type().is_symlink() || !m.is_file() {
                return Err(Error::Corrupt(
                    "partition artifact is not a regular file".into(),
                ));
            }
            if (name == "manifest.json" || name == "manifest.tmp")
                && m.len() > limits.partition.max_manifest_bytes
            {
                return Err(Error::Corrupt("partition metadata exceeds bound".into()));
            }
            if name == "events.jsonl" && m.len() > limits.partition.max_partition_bytes {
                return Err(Error::Corrupt("partition events exceed bound".into()));
            }
            if matches!(name.as_str(), "events.jsonl.gz" | "events.jsonl.gz.tmp")
                && m.len() > crate::max_compressed_bytes(&limits.partition)?
            {
                return Err(Error::Corrupt(
                    "compressed partition events exceed bound".into(),
                ));
            }
            names.push(name);
        }
    }

    let complete_path = dir.join("complete.json");
    let complete_exists = root_names.iter().any(|n| n == "complete.json");
    let mut committed = Vec::new();
    let (mut used_bytes, mut used_records, mut previous_seq, mut previous_elapsed) =
        (0u64, 0u64, None, None);
    let mut uncommitted = None;
    for ordinal in &ordinals {
        let partition_id = ordinal_id(*ordinal);
        let remaining_bytes = limits
            .max_total_event_bytes
            .checked_sub(used_bytes)
            .ok_or_else(|| Error::Corrupt("aggregate byte overflow".into()))?;
        let remaining_records = limits
            .max_total_records
            .checked_sub(used_records)
            .ok_or_else(|| Error::Corrupt("aggregate record overflow".into()))?;
        if remaining_bytes == 0 || remaining_records == 0 {
            return Err(Error::Limit("recording aggregate exhausted".into()));
        }
        let mut part_limits = limits.partition.clone();
        part_limits.max_partition_bytes = part_limits.max_partition_bytes.min(remaining_bytes);
        part_limits.max_records = part_limits.max_records.min(remaining_records);
        match crate::load(&dir, &partition_id, &part_limits) {
            Ok((manifest, events)) => {
                if uncommitted.is_some() {
                    return Err(Error::Corrupt(
                        "committed partition follows uncommitted tail".into(),
                    ));
                }
                if manifest.collector_id != start.scope.collector_id
                    || manifest.collector_session_id != start.scope.session_id
                {
                    return Err(Error::Corrupt("partition scope mismatch".into()));
                }
                for (index, event) in events.iter().enumerate() {
                    if event.collector_region != start.scope.collector_region {
                        return Err(Error::Corrupt("partition region mismatch".into()));
                    }
                    let expected = previous_seq
                        .map_or(Some(1), |n: u64| n.checked_add(1))
                        .ok_or_else(|| Error::Corrupt("sequence overflow".into()))?;
                    if event.ingest_sequence != expected
                        || previous_elapsed.is_some_and(|n| event.monotonic_elapsed_nanos < n)
                    {
                        return Err(Error::Corrupt(
                            "cross-partition sequence or monotonic regression".into(),
                        ));
                    }
                    if index == 0 && event.ingest_sequence != manifest.first_ingest_sequence {
                        return Err(Error::Corrupt("partition first sequence mismatch".into()));
                    }
                    previous_seq = Some(event.ingest_sequence);
                    previous_elapsed = Some(event.monotonic_elapsed_nanos);
                }
                used_bytes = used_bytes
                    .checked_add(manifest.event_bytes)
                    .ok_or_else(|| Error::Corrupt("aggregate bytes overflow".into()))?;
                used_records = used_records
                    .checked_add(manifest.record_count)
                    .ok_or_else(|| Error::Corrupt("aggregate records overflow".into()))?;
                committed.push(manifest);
            }
            Err(Error::Uncommitted) if uncommitted.is_none() => {
                uncommitted = Some(partition_id);
            }
            Err(Error::Uncommitted) => {
                return Err(Error::Corrupt("multiple uncommitted partitions".into()));
            }
            Err(e) => return Err(e),
        }
    }

    if complete_exists {
        if committed.is_empty() {
            return Err(Error::Corrupt("empty completed recording".into()));
        }
        if uncommitted.is_some() {
            return Err(Error::Corrupt("completion with uncommitted tail".into()));
        }
        let bytes = crate::bounded_file(&complete_path, limits.max_metadata_bytes)?;
        let end: End = serde_json::from_slice(&bytes).map_err(|e| Error::Corrupt(e.to_string()))?;
        if end.version != VERSION
            || end.start_sha256 != start_sha
            || end.id != id
            || end.scope != start.scope
            || end.partitions.len() != committed.len()
            || end.event_bytes != used_bytes
            || end.records != used_records
        {
            return Err(Error::Corrupt(
                "completion totals or identity mismatch".into(),
            ));
        }
        if end.partitions != committed {
            return Err(Error::Corrupt(
                "completion partition reference mismatch".into(),
            ));
        }
        let receipt = CompletionReceipt {
            recording_id: id.into(),
            scope: start.scope.clone(),
            start_sha256: start_sha.clone(),
            partitions: committed,
            event_bytes: used_bytes,
            records: used_records,
        };
        Ok(Report {
            recording_id: id.into(),
            scope: start.scope,
            start_sha256: start_sha,
            status: Status::Complete(receipt),
        })
    } else {
        Ok(Report {
            recording_id: id.into(),
            scope: start.scope,
            start_sha256: start_sha,
            status: Status::Interrupted {
                committed,
                uncommitted_partition: uncommitted,
            },
        })
    }
}
