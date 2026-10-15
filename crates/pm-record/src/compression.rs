//! Opt-in post-seal gzip packing for immutable v1 event partitions.
use crate::{
    Error, Limits, Manifest, load, max_compressed_bytes, optional_regular_file, trusted_dir,
    valid_id,
};
use flate2::{Compression, write::GzEncoder};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::Path,
};

const RAW_NAME: &str = "events.jsonl";
const GZIP_NAME: &str = "events.jsonl.gz";
const TEMP_NAME: &str = "events.jsonl.gz.tmp";

/// Result of packing; manifest byte counts and checksum continue to describe logical JSONL.
#[derive(Debug)]
pub struct Report {
    pub manifest: Manifest,
    pub compressed_bytes: u64,
}

struct LimitedFile {
    file: File,
    bytes: u64,
    limit: u64,
}

impl Write for LimitedFile {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if input.is_empty() {
            return Ok(0);
        }
        let remaining = self.limit.saturating_sub(self.bytes);
        if remaining == 0 {
            return Err(io::Error::other("compressed partition bound exceeded"));
        }
        let allowed = input
            .len()
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let chunk = input
            .get(..allowed)
            .ok_or_else(|| io::Error::other("compressed writer slice unavailable"))?;
        let written = self.file.write(chunk)?;
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(written).unwrap_or(u64::MAX))
            .ok_or_else(|| io::Error::other("compressed byte count overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// Validate a committed partition and publish a bounded single-member gzip copy.
/// The caller must ensure the trusted storage root is quiescent for this operation.
pub fn pack(root: impl AsRef<Path>, id: &str, limits: &Limits) -> Result<Report, Error> {
    let root = root.as_ref();
    let (manifest, events) = load(root, id, limits)?;
    drop(events);

    if !valid_id(id) {
        return Err(Error::Invalid("invalid partition id".into()));
    }
    trusted_dir(root)?;
    let dir = root.join(id);
    trusted_dir(&dir)?;
    let raw_path = dir.join(RAW_NAME);
    let gzip_path = dir.join(GZIP_NAME);
    let temp_path = dir.join(TEMP_NAME);
    let raw_meta = optional_regular_file(&raw_path, "events")?;
    let gzip_meta = optional_regular_file(&gzip_path, "compressed events")?;
    let temp_meta = optional_regular_file(&temp_path, "compression temporary")?;
    if let Some(meta) = &temp_meta {
        if meta.len() > max_compressed_bytes(limits)? {
            return Err(Error::Corrupt("compression temporary exceeds bound".into()));
        }
        let is_published_link = gzip_meta.as_ref().is_some_and(|gzip| same_file(meta, gzip));
        if !is_published_link {
            return Err(Error::Corrupt(
                "compression temporary already exists".into(),
            ));
        }
        fs::remove_file(&temp_path)?;
        sync_directory(&dir)?;
    }

    match (raw_meta.is_some(), gzip_meta.is_some()) {
        (false, true) => {
            File::open(&gzip_path)?.sync_all()?;
            sync_directory(&dir)?;
            let compressed_bytes = gzip_meta.map_or(0, |meta| meta.len());
            Ok(Report {
                manifest,
                compressed_bytes,
            })
        }
        (true, true) => {
            File::open(&gzip_path)?.sync_all()?;
            sync_directory(&dir)?;
            fs::remove_file(&raw_path)?;
            sync_directory(&dir)?;
            let compressed_bytes = gzip_meta.map_or(0, |meta| meta.len());
            Ok(Report {
                manifest,
                compressed_bytes,
            })
        }
        (true, false) => pack_raw(
            root, &dir, &raw_path, &gzip_path, &temp_path, manifest, limits,
        ),
        (false, false) => Err(Error::Corrupt("committed manifest missing events".into())),
    }
}

fn pack_raw(
    root: &Path,
    dir: &Path,
    raw_path: &Path,
    gzip_path: &Path,
    temp_path: &Path,
    manifest: Manifest,
    limits: &Limits,
) -> Result<Report, Error> {
    let compressed_limit = max_compressed_bytes(limits)?;
    let temp = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temp_path)?;
    let bounded = LimitedFile {
        file: temp,
        bytes: 0,
        limit: compressed_limit,
    };
    let mut encoder = GzEncoder::new(bounded, Compression::new(1));
    let raw = File::open(raw_path)?;
    let mut input = raw.take(manifest.event_bytes);
    let copied = io::copy(&mut input, &mut encoder)?;
    if copied != manifest.event_bytes {
        return Err(Error::Corrupt(
            "raw event size changed during packing".into(),
        ));
    }
    let bounded = encoder.finish()?;
    let compressed_bytes = bounded.bytes;
    bounded.file.sync_all()?;
    drop(bounded);

    fs::hard_link(temp_path, gzip_path)?;
    sync_directory(dir)?;
    fs::remove_file(temp_path)?;
    sync_directory(dir)?;

    let (verified_manifest, events) = load(root, &manifest.partition_id, limits)?;
    drop(events);
    if verified_manifest != manifest {
        return Err(Error::Corrupt("manifest changed during packing".into()));
    }
    fs::remove_file(raw_path)?;
    sync_directory(dir)?;
    Ok(Report {
        manifest,
        compressed_bytes,
    })
}

fn sync_directory(path: &Path) -> Result<(), Error> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn same_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_file(_left: &fs::Metadata, _right: &fs::Metadata) -> bool {
    false
}
