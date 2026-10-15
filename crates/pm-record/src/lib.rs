use chrono::{DateTime, Utc};
use pm_schema::EventEnvelope;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
};

pub mod compression;

#[derive(Clone, Debug)]
pub struct Limits {
    pub max_payload_bytes: u64,
    pub max_record_bytes: u64,
    pub max_partition_bytes: u64,
    pub max_records: u64,
    pub max_manifest_bytes: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_payload_bytes: 1_048_576,
            max_record_bytes: 2_097_152,
            max_partition_bytes: 1_073_741_824,
            max_records: 100_000,
            max_manifest_bytes: 65_536,
        }
    }
}
impl Limits {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if [
            self.max_payload_bytes,
            self.max_record_bytes,
            self.max_partition_bytes,
            self.max_records,
            self.max_manifest_bytes,
        ]
        .contains(&0)
        {
            return Err(Error::Invalid("all limits must be positive".into()));
        }
        usize::try_from(self.max_record_bytes)
            .map_err(|_| Error::Invalid("record limit exceeds address space".into()))?;
        usize::try_from(
            self.max_manifest_bytes
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("manifest limit overflow".into()))?,
        )
        .map_err(|_| Error::Invalid("manifest limit exceeds address space".into()))?;
        Ok(())
    }
}
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Invalid(String),
    Corrupt(String),
    Uncommitted,
    Limit(String),
    Poisoned,
    Sealed,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub format_version: u16,
    pub partition_id: String,
    pub event_bytes: u64,
    pub record_count: u64,
    pub sha256: String,
    pub collector_id: String,
    pub collector_session_id: String,
    pub first_ingest_sequence: u64,
    pub last_ingest_sequence: u64,
    pub min_available_at: DateTime<Utc>,
    pub max_available_at: DateTime<Utc>,
}
#[derive(Debug)]
pub struct Receipt {
    pub manifest: Manifest,
}
/// Synchronized bytes of an open partition, not a published or complete partition.
/// Later appends may extend it; ordinary archive loaders still require [`Receipt`].
#[derive(Debug)]
pub struct DurablePrefix {
    pub manifest: Manifest,
}
#[derive(Debug)]
pub enum Inspection {
    Committed(Manifest),
    Uncommitted,
    Corrupt(String),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Life {
    Open,
    Sealed,
    Poisoned,
}
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FaultAt {
    PartialAppend,
    Flush,
    EventSync,
    ManifestWrite,
    ManifestSync,
    Publish,
    PartitionSync,
    RootSync,
}
pub struct Writer {
    #[cfg(test)]
    fault: Option<FaultAt>,
    root: PathBuf,
    dir: PathBuf,
    file: Option<File>,
    limits: Limits,
    manifest: Manifest,
    bytes: u64,
    elapsed: u64,
    hash: Sha256,
    life: Life,
}
pub(crate) const GZIP_OVERHEAD_CAP: u64 = 64 * 1024;

pub(crate) fn max_compressed_bytes(limits: &Limits) -> Result<u64, Error> {
    limits
        .max_partition_bytes
        .checked_add(GZIP_OVERHEAD_CAP)
        .ok_or_else(|| Error::Invalid("compressed partition bound overflow".into()))
}
fn valid_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}
fn partition_dir_error(e: Error) -> Error {
    match e {
        Error::Io(x) if x.kind() == io::ErrorKind::NotFound => Error::Uncommitted,
        Error::Io(x) => Error::Io(x),
        Error::Invalid(s) => Error::Corrupt(s),
        other => other,
    }
}
fn trusted_dir(path: &Path) -> Result<(), Error> {
    let m = fs::symlink_metadata(path)?;
    if !m.is_dir() || m.file_type().is_symlink() {
        return Err(Error::Invalid("expected non-symlink directory".into()));
    }
    Ok(())
}
struct Bounded(Vec<u8>, usize);
impl Write for Bounded {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        let n = self
            .0
            .len()
            .checked_add(b.len())
            .ok_or_else(|| io::Error::other("length overflow"))?;
        if n > self.1 {
            return Err(io::Error::other("bounded serialization limit"));
        }
        self.0.extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(crate) fn encode_event(e: &EventEnvelope, limits: &Limits) -> Result<Vec<u8>, Error> {
    let cap = usize::try_from(limits.max_record_bytes)
        .map_err(|_| Error::Invalid("record bound".into()))?;
    let mut b = Bounded(
        Vec::new(),
        cap.checked_sub(1)
            .ok_or_else(|| Error::Invalid("record limit too small".into()))?,
    );
    serde_json::to_writer(&mut b, e).map_err(|x| Error::Limit(x.to_string()))?;
    b.0.push(b'\n');
    Ok(b.0)
}
impl Writer {
    pub fn create(root: impl AsRef<Path>, id: &str, limits: Limits) -> Result<Self, Error> {
        limits.validate()?;
        let root = root.as_ref();
        trusted_dir(root)?;
        if !valid_id(id) {
            return Err(Error::Invalid("invalid partition id".into()));
        }
        let dir = root.join(id);
        fs::create_dir(&dir)?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join("events.jsonl"))?;
        Ok(Self {
            #[cfg(test)]
            fault: None,
            root: root.into(),
            dir,
            file: Some(file),
            limits,
            manifest: Manifest {
                format_version: 1,
                partition_id: id.into(),
                event_bytes: 0,
                record_count: 0,
                sha256: String::new(),
                collector_id: String::new(),
                collector_session_id: String::new(),
                first_ingest_sequence: 0,
                last_ingest_sequence: 0,
                min_available_at: DateTime::from_timestamp(0, 0)
                    .ok_or_else(|| Error::Invalid("epoch unavailable".into()))?,
                max_available_at: DateTime::from_timestamp(0, 0)
                    .ok_or_else(|| Error::Invalid("epoch unavailable".into()))?,
            },
            bytes: 0,
            elapsed: 0,
            hash: Sha256::new(),
            life: Life::Open,
        })
    }
    #[cfg(test)]
    fn fail_if(&mut self, point: FaultAt) -> Result<(), Error> {
        if self.fault == Some(point) {
            self.fault = None;
            return Err(Error::Io(io::Error::other(format!("injected {point:?}"))));
        }
        Ok(())
    }
    pub fn append(&mut self, e: &EventEnvelope) -> Result<(), Error> {
        self.append_with_bytes(e, None)
    }
    pub(crate) fn append_encoded(
        &mut self,
        e: &EventEnvelope,
        encoded: Vec<u8>,
    ) -> Result<(), Error> {
        self.append_with_bytes(e, Some(encoded))
    }
    fn append_with_bytes(
        &mut self,
        e: &EventEnvelope,
        encoded: Option<Vec<u8>>,
    ) -> Result<(), Error> {
        if self.life == Life::Sealed {
            return Err(Error::Sealed);
        }
        if self.life == Life::Poisoned {
            return Err(Error::Poisoned);
        }
        let r = self.append_inner(e, encoded);
        if matches!(r, Err(Error::Io(_))) {
            self.life = Life::Poisoned
        }
        r
    }
    fn append_inner(&mut self, e: &EventEnvelope, encoded: Option<Vec<u8>>) -> Result<(), Error> {
        e.validate().map_err(|x| Error::Invalid(x.to_string()))?;
        if u64::try_from(e.raw_payload.len()).unwrap_or(u64::MAX) > self.limits.max_payload_bytes {
            return Err(Error::Limit("payload".into()));
        }
        if self.manifest.record_count >= self.limits.max_records {
            return Err(Error::Limit("records".into()));
        }
        if self.manifest.record_count > 0
            && (e.collector_id != self.manifest.collector_id
                || e.collector_session_id != self.manifest.collector_session_id
                || e.ingest_sequence
                    != self
                        .manifest
                        .last_ingest_sequence
                        .checked_add(1)
                        .ok_or_else(|| Error::Invalid("sequence overflow".into()))?
                || e.monotonic_elapsed_nanos < self.elapsed)
        {
            return Err(Error::Invalid(
                "identity, sequence or monotonic order".into(),
            ));
        }
        let encoded = match encoded {
            Some(v) => v,
            None => encode_event(e, &self.limits)?,
        };
        let n = self
            .bytes
            .checked_add(
                u64::try_from(encoded.len())
                    .map_err(|_| Error::Limit("record size overflow".into()))?,
            )
            .ok_or_else(|| Error::Limit("partition byte overflow".into()))?;
        if n > self.limits.max_partition_bytes {
            return Err(Error::Limit("partition".into()));
        }
        #[cfg(test)]
        if self.fault == Some(FaultAt::PartialAppend) {
            self.fault = None;
            let split = encoded.len().min(3);
            let partial = encoded
                .get(..split)
                .ok_or_else(|| Error::Io(io::Error::other("injected slice unavailable")))?;
            self.file
                .as_mut()
                .ok_or(Error::Poisoned)?
                .write_all(partial)?;
            return Err(Error::Io(io::Error::other("injected partial append")));
        }
        self.file
            .as_mut()
            .ok_or(Error::Poisoned)?
            .write_all(&encoded)?;
        self.hash.update(&encoded);
        self.bytes = n;
        if self.manifest.record_count == 0 {
            self.manifest.collector_id = e.collector_id.clone();
            self.manifest.collector_session_id = e.collector_session_id.clone();
            self.manifest.first_ingest_sequence = e.ingest_sequence;
            self.manifest.min_available_at = e.available_at;
            self.manifest.max_available_at = e.available_at;
        } else {
            self.manifest.min_available_at = self.manifest.min_available_at.min(e.available_at);
            self.manifest.max_available_at = self.manifest.max_available_at.max(e.available_at)
        }
        self.manifest.last_ingest_sequence = e.ingest_sequence;
        self.elapsed = e.monotonic_elapsed_nanos;
        self.manifest.record_count = self
            .manifest
            .record_count
            .checked_add(1)
            .ok_or_else(|| Error::Limit("record count overflow".into()))?;
        Ok(())
    }
    /// Synchronize the current nonempty prefix without publishing a final manifest.
    /// An I/O failure poisons the writer. This acknowledges local storage only;
    /// it does not qualify event timing or make an unfinished partition loadable.
    pub fn sync_prefix(&mut self) -> Result<DurablePrefix, Error> {
        match self.life {
            Life::Poisoned => return Err(Error::Poisoned),
            Life::Sealed => return Err(Error::Sealed),
            Life::Open => {}
        }
        if self.manifest.record_count == 0 {
            return Err(Error::Invalid("cannot synchronize an empty prefix".into()));
        }
        let result = (|| -> Result<(), Error> {
            #[cfg(test)]
            self.fail_if(FaultAt::Flush)?;
            self.file.as_mut().ok_or(Error::Poisoned)?.flush()?;
            #[cfg(test)]
            self.fail_if(FaultAt::EventSync)?;
            self.file.as_ref().ok_or(Error::Poisoned)?.sync_all()?;
            #[cfg(test)]
            self.fail_if(FaultAt::PartitionSync)?;
            File::open(&self.dir)?.sync_all()?;
            #[cfg(test)]
            self.fail_if(FaultAt::RootSync)?;
            File::open(&self.root)?.sync_all()?;
            Ok(())
        })();
        if let Err(error) = result {
            self.life = Life::Poisoned;
            return Err(error);
        }
        let mut manifest = self.manifest.clone();
        manifest.event_bytes = self.bytes;
        manifest.sha256 = hex::encode(self.hash.clone().finalize());
        Ok(DurablePrefix { manifest })
    }

    pub fn seal(&mut self) -> Result<Receipt, Error> {
        if self.life == Life::Sealed {
            return Err(Error::Sealed);
        }
        if self.life == Life::Poisoned {
            return Err(Error::Poisoned);
        }
        if self.manifest.record_count == 0 {
            return Err(Error::Invalid("empty partition".into()));
        }
        let r = self.seal_inner();
        match r {
            Ok(receipt) => {
                self.life = Life::Sealed;
                Ok(receipt)
            }
            Err(e) => {
                self.life = Life::Poisoned;
                Err(e)
            }
        }
    }
    fn seal_inner(&mut self) -> Result<Receipt, Error> {
        let mut f = self.file.take().ok_or(Error::Poisoned)?;
        #[cfg(test)]
        self.fail_if(FaultAt::Flush)?;
        f.flush()?;
        #[cfg(test)]
        self.fail_if(FaultAt::EventSync)?;
        f.sync_all()?;
        self.manifest.event_bytes = self.bytes;
        self.manifest.sha256 = hex::encode(self.hash.clone().finalize());
        let cap = usize::try_from(self.limits.max_manifest_bytes)
            .map_err(|_| Error::Invalid("manifest bound".into()))?;
        let mut data = Bounded(Vec::new(), cap);
        serde_json::to_writer(&mut data, &self.manifest)
            .map_err(|x| Error::Limit(x.to_string()))?;
        let tmp = self.dir.join("manifest.tmp");
        let mut mf = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        #[cfg(test)]
        self.fail_if(FaultAt::ManifestWrite)?;
        mf.write_all(&data.0)?;
        mf.flush()?;
        #[cfg(test)]
        self.fail_if(FaultAt::ManifestSync)?;
        mf.sync_all()?;
        drop(mf);
        #[cfg(test)]
        self.fail_if(FaultAt::Publish)?;
        fs::hard_link(&tmp, self.dir.join("manifest.json"))?;
        #[cfg(test)]
        self.fail_if(FaultAt::PartitionSync)?;
        File::open(&self.dir)?.sync_all()?;
        #[cfg(test)]
        self.fail_if(FaultAt::RootSync)?;
        File::open(&self.root)?.sync_all()?;
        Ok(Receipt {
            manifest: self.manifest.clone(),
        })
    }
}
fn bounded_file(path: &Path, max: u64) -> Result<Vec<u8>, Error> {
    let m = fs::symlink_metadata(path)?;
    if !m.is_file() || m.file_type().is_symlink() {
        return Err(Error::Corrupt("expected regular file".into()));
    }
    if m.len() > max {
        return Err(Error::Corrupt("file exceeds configured bound".into()));
    }
    let cap = max
        .checked_add(1)
        .ok_or_else(|| Error::Invalid("read bound overflow".into()))?;
    let mut v = Vec::new();
    File::open(path)?.take(cap).read_to_end(&mut v)?;
    if u64::try_from(v.len()).unwrap_or(u64::MAX) > max {
        return Err(Error::Corrupt("file exceeds configured bound".into()));
    }
    Ok(v)
}
pub fn load(
    root: impl AsRef<Path>,
    id: &str,
    limits: &Limits,
) -> Result<(Manifest, Vec<EventEnvelope>), Error> {
    limits.validate()?;
    if !valid_id(id) {
        return Err(Error::Invalid("invalid partition id".into()));
    }
    let root = root.as_ref();
    trusted_dir(root)?;
    let dir = root.join(id);
    trusted_dir(&dir).map_err(partition_dir_error)?;
    let mp = dir.join("manifest.json");
    let manifest_bytes = match bounded_file(&mp, limits.max_manifest_bytes) {
        Ok(v) => v,
        Err(Error::Io(e)) if e.kind() == io::ErrorKind::NotFound => return Err(Error::Uncommitted),
        Err(e) => return Err(e),
    };
    let manifest: Manifest =
        serde_json::from_slice(&manifest_bytes).map_err(|e| Error::Corrupt(e.to_string()))?;
    if manifest.format_version != 1
        || manifest.partition_id != id
        || manifest.record_count == 0
        || manifest.record_count > limits.max_records
        || manifest.event_bytes == 0
        || manifest.event_bytes > limits.max_partition_bytes
        || !valid_id(&manifest.partition_id)
        || manifest.first_ingest_sequence == 0
        || manifest.collector_id.is_empty()
        || manifest.collector_session_id.is_empty()
        || manifest.sha256.len() != 64
        || hex::decode(&manifest.sha256).is_err()
        || manifest.min_available_at > manifest.max_available_at
    {
        return Err(Error::Corrupt("manifest fields inconsistent".into()));
    }
    let raw_path = dir.join("events.jsonl");
    let gzip_path = dir.join("events.jsonl.gz");
    let raw_meta = optional_regular_file(&raw_path, "events")?;
    let gzip_meta = optional_regular_file(&gzip_path, "compressed events")?;
    if raw_meta.is_none() && gzip_meta.is_none() {
        return Err(Error::Corrupt("committed manifest missing events".into()));
    }
    if let Some(meta) = &raw_meta {
        if meta.len() != manifest.event_bytes || meta.len() > limits.max_partition_bytes {
            return Err(Error::Corrupt("event byte size mismatch".into()));
        }
    }
    if let Some(meta) = &gzip_meta {
        if meta.len() > max_compressed_bytes(limits)? {
            return Err(Error::Corrupt("compressed event file exceeds bound".into()));
        }
    }

    let records = if raw_meta.is_some() {
        let raw_file = File::open(&raw_path)?;
        let mut raw_reader = BufReader::new(raw_file);
        let records = validate_events(&mut raw_reader, &manifest, limits, None)?;
        if gzip_meta.is_some() {
            let raw_file = File::open(&raw_path)?;
            let mut raw_reader = BufReader::new(raw_file);
            let comparison_records =
                validate_gzip_events(&gzip_path, &manifest, limits, Some(&mut raw_reader))?;
            if !comparison_records.is_empty() {
                return Err(Error::Corrupt("compressed event comparison failed".into()));
            }
        }
        records
    } else {
        validate_gzip_events(&gzip_path, &manifest, limits, None)?
    };
    Ok((manifest, records))
}

fn optional_regular_file(path: &Path, label: &str) -> Result<Option<fs::Metadata>, Error> {
    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::Io(error)),
    };
    if !meta.is_file() || meta.file_type().is_symlink() {
        return Err(Error::Corrupt(format!("{label} not regular file")));
    }
    Ok(Some(meta))
}

fn validate_gzip_events(
    path: &Path,
    manifest: &Manifest,
    limits: &Limits,
    other: Option<&mut dyn BufRead>,
) -> Result<Vec<EventEnvelope>, Error> {
    use flate2::bufread::GzDecoder;

    let file = File::open(path)?;
    let compressed = BufReader::new(file);
    let decoder = GzDecoder::new(compressed);
    let decoded_limit = manifest
        .event_bytes
        .checked_add(1)
        .ok_or_else(|| Error::Corrupt("decompressed event bound overflow".into()))?;
    let limited = decoder.take(decoded_limit);
    let mut reader = BufReader::new(limited);
    let records = validate_events(&mut reader, manifest, limits, other).map_err(gzip_read_error)?;
    let limited = reader.into_inner();
    let decoder = limited.into_inner();
    let mut compressed = decoder.into_inner();
    if !compressed
        .fill_buf()
        .map_err(|error| Error::Corrupt(format!("invalid gzip stream: {error}")))?
        .is_empty()
    {
        return Err(Error::Corrupt(
            "trailing bytes or concatenated gzip member".into(),
        ));
    }
    Ok(records)
}

fn gzip_read_error(error: Error) -> Error {
    match error {
        Error::Io(error) => Error::Corrupt(format!("invalid gzip stream: {error}")),
        other => other,
    }
}

fn validate_events(
    reader: &mut impl BufRead,
    manifest: &Manifest,
    limits: &Limits,
    mut other: Option<&mut dyn BufRead>,
) -> Result<Vec<EventEnvelope>, Error> {
    let mut records = Vec::new();
    let mut line = Vec::new();
    let mut other_line = Vec::new();
    let mut bytes = 0u64;
    let mut count = 0u64;
    let mut hash = Sha256::new();
    let mut prev_seq: Option<u64> = None;
    let mut elapsed: Option<u64> = None;
    let mut min = None;
    let mut max = None;
    loop {
        line.clear();
        let n = read_line_bounded(reader, &mut line, limits.max_record_bytes)?;
        if n == 0 {
            break;
        }
        if let Some(other_reader) = other.as_deref_mut() {
            other_line.clear();
            let other_n =
                read_line_bounded(other_reader, &mut other_line, limits.max_record_bytes)?;
            if other_n != n || other_line != line {
                return Err(Error::Corrupt(
                    "raw and compressed event streams differ".into(),
                ));
            }
        }
        let n = u64::try_from(n).map_err(|_| Error::Corrupt("line size overflow".into()))?;
        bytes = bytes
            .checked_add(n)
            .ok_or_else(|| Error::Corrupt("byte overflow".into()))?;
        if bytes > manifest.event_bytes || bytes > limits.max_partition_bytes {
            return Err(Error::Corrupt("event byte limit exceeded".into()));
        }
        if count >= manifest.record_count || count >= limits.max_records {
            return Err(Error::Corrupt("record count exceeded".into()));
        }
        hash.update(&line);
        if line.last() != Some(&b'\n') {
            return Err(Error::Corrupt("missing newline".into()));
        }
        line.pop();
        let e: EventEnvelope =
            serde_json::from_slice(&line).map_err(|x| Error::Corrupt(x.to_string()))?;
        e.validate().map_err(|x| Error::Corrupt(x.to_string()))?;
        if u64::try_from(e.raw_payload.len()).unwrap_or(u64::MAX) > limits.max_payload_bytes {
            return Err(Error::Corrupt("payload limit".into()));
        }
        let expected = match prev_seq {
            None => manifest.first_ingest_sequence,
            Some(p) => p
                .checked_add(1)
                .ok_or_else(|| Error::Corrupt("sequence overflow".into()))?,
        };
        if e.ingest_sequence != expected
            || e.collector_id != manifest.collector_id
            || e.collector_session_id != manifest.collector_session_id
            || elapsed.is_some_and(|x| e.monotonic_elapsed_nanos < x)
        {
            return Err(Error::Corrupt("identity or ordering mismatch".into()));
        }
        prev_seq = Some(e.ingest_sequence);
        elapsed = Some(e.monotonic_elapsed_nanos);
        min = Some(min.map_or(e.available_at, |x: DateTime<Utc>| x.min(e.available_at)));
        max = Some(max.map_or(e.available_at, |x: DateTime<Utc>| x.max(e.available_at)));
        if other.is_none() {
            records.push(e);
        }
        count = count
            .checked_add(1)
            .ok_or_else(|| Error::Corrupt("record count overflow".into()))?
    }
    if let Some(other_reader) = other {
        other_line.clear();
        if read_line_bounded(other_reader, &mut other_line, limits.max_record_bytes)? != 0 {
            return Err(Error::Corrupt(
                "raw and compressed event streams differ".into(),
            ));
        }
    }
    if count != manifest.record_count
        || bytes != manifest.event_bytes
        || hex::encode(hash.finalize()) != manifest.sha256
        || prev_seq != Some(manifest.last_ingest_sequence)
        || min != Some(manifest.min_available_at)
        || max != Some(manifest.max_available_at)
    {
        return Err(Error::Corrupt("manifest/content mismatch".into()));
    }
    Ok(records)
}
fn read_line_bounded(r: &mut dyn BufRead, out: &mut Vec<u8>, limit: u64) -> Result<usize, Error> {
    let mut total = 0usize;
    loop {
        let available = r.fill_buf()?;
        if available.is_empty() {
            return Ok(total);
        }
        let take = available
            .iter()
            .position(|b| *b == b'\n')
            .map_or(available.len(), |i| i + 1);
        let new = total
            .checked_add(take)
            .ok_or_else(|| Error::Corrupt("line length overflow".into()))?;
        if u64::try_from(new).unwrap_or(u64::MAX) > limit {
            return Err(Error::Corrupt("record exceeds limit".into()));
        }
        let done = available
            .get(..take)
            .ok_or_else(|| Error::Corrupt("buffer slice".into()))?;
        let newline = done.last() == Some(&b'\n');
        out.extend_from_slice(done);
        r.consume(take);
        total = new;
        if newline {
            return Ok(total);
        }
    }
}
pub mod recording;

#[cfg(test)]
#[path = "recording_tests.rs"]
mod recording_tests;

#[cfg(test)]
#[path = "fault_tests.rs"]
mod fault_tests;

pub fn inspect(root: impl AsRef<Path>, id: &str, limits: &Limits) -> Result<Inspection, Error> {
    match load(root, id, limits) {
        Ok((m, _)) => Ok(Inspection::Committed(m)),
        Err(Error::Uncommitted) => Ok(Inspection::Uncommitted),
        Err(Error::Corrupt(s)) => Ok(Inspection::Corrupt(s)),
        Err(e) => Err(e),
    }
}
