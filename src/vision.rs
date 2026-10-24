//! Cached, checksum-verified access to the public data.binance.vision archive
//! (USD-M futures and spot) plus CSV parsers for klines, metrics and funding.
//!
//! The cache mirrors the URL path: `<cache>/<rel>` is the zip, `<rel>.CHECKSUM`
//! its sha256 file and `<rel>.404` a negative entry. A missing file is normal
//! (before listing, after delisting) and never an error. Entries are written to a
//! temporary name and renamed, so an interrupted run resumes cleanly.

use std::{
    io::Read,
    path::{Path, PathBuf},
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use chrono::{NaiveDate, NaiveDateTime};
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest, Sha256};

pub const DEFAULT_BASE_URL: &str = "https://data.binance.vision";
pub const DEFAULT_LISTING_URL: &str = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision";
const USER_AGENT: &str =
    "cryptomarket-data-vision/0.1 (public archive reader; bounded concurrency)";
const MAX_UNZIPPED_BYTES: u64 = 512 * 1024 * 1024;
const MAX_RETRY_AFTER_SECS: u64 = 300;
const DAY_SECS: u64 = 86_400;

#[derive(Debug)]
pub enum VisionError {
    /// The configured download budget would be exceeded.
    Budget {
        limit: u64,
        needed: u64,
    },
    Checksum(String),
    Http(String),
    Io(String),
    Parse(String),
}
impl std::fmt::Display for VisionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Budget { limit, needed } => write!(
                f,
                "download budget exceeded: {needed} bytes needed, limit {limit} bytes \
                 (raise the download limit; the cache keeps completed files)"
            ),
            Self::Checksum(m) => write!(f, "checksum: {m}"),
            Self::Http(m) => write!(f, "http: {m}"),
            Self::Io(m) => write!(f, "io: {m}"),
            Self::Parse(m) => write!(f, "parse: {m}"),
        }
    }
}
impl std::error::Error for VisionError {}
impl From<std::io::Error> for VisionError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}
impl From<reqwest::Error> for VisionError {
    fn from(e: reqwest::Error) -> Self {
        Self::Http(e.to_string())
    }
}
impl From<zip::result::ZipError> for VisionError {
    fn from(e: zip::result::ZipError) -> Self {
        Self::Parse(format!("zip: {e}"))
    }
}
pub type VisionResult<T> = Result<T, VisionError>;

#[derive(Clone, Debug)]
pub struct VisionConfig {
    pub base_url: String,
    pub listing_url: String,
    pub cache_dir: PathBuf,
    pub offline: bool,
    pub concurrency: usize,
    pub max_download_bytes: u64,
    pub max_attempts: u32,
    pub backoff_ms: u64,
    pub timeout_secs: u64,
}
impl VisionConfig {
    pub fn new(cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.into(),
            listing_url: DEFAULT_LISTING_URL.into(),
            cache_dir: cache_dir.into(),
            offline: false,
            concurrency: 4,
            max_download_bytes: 3000 * 1024 * 1024,
            max_attempts: 5,
            backoff_ms: 500,
            timeout_secs: 60,
        }
    }
}

#[derive(Default)]
struct Stats {
    requests: AtomicU64,
    cache_hits: AtomicU64,
    negative_hits: AtomicU64,
    not_found: AtomicU64,
    retries: AtomicU64,
    offline_misses: AtomicU64,
    bytes_downloaded: AtomicU64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StatsSnapshot {
    pub requests: u64,
    pub cache_hits: u64,
    pub negative_cache_hits: u64,
    pub not_found_responses: u64,
    pub retries: u64,
    pub offline_misses: u64,
    pub bytes_downloaded: u64,
}

pub struct Vision {
    cfg: VisionConfig,
    client: reqwest::Client,
    permits: tokio::sync::Semaphore,
    stats: Stats,
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn append(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// Date a file name refers to, with the number of days after which it must exist.
fn file_date(rel: &str) -> Option<(NaiveDate, i64)> {
    let stem = rel.rsplit('/').next()?.strip_suffix(".zip")?;
    let parts: Vec<&str> = stem.split('-').collect();
    let back = |k: usize| {
        parts
            .len()
            .checked_sub(k)
            .and_then(|i| parts.get(i))
            .copied()
    };
    if let (Some(y), Some(m), Some(d)) = (back(3), back(2), back(1)) {
        if let (Ok(y), Ok(m), Ok(d)) = (y.parse::<i32>(), m.parse::<u32>(), d.parse::<u32>()) {
            return Some((NaiveDate::from_ymd_opt(y, m, d)?, 1));
        }
    }
    if let (Some(y), Some(m)) = (back(2), back(1)) {
        if let (Ok(y), Ok(m)) = (y.parse::<i32>(), m.parse::<u32>()) {
            return Some((NaiveDate::from_ymd_opt(y, m, 1)?, 31));
        }
    }
    None
}

/// A negative entry is permanent once it was recorded well after the file should
/// have been published; a recent one (file possibly not yet published) lasts a day.
fn negative_valid(rel: &str, recorded_secs: u64, now: u64) -> bool {
    if now.saturating_sub(recorded_secs) < DAY_SECS {
        return true;
    }
    file_date(rel).is_some_and(|(date, span)| {
        let end = date
            .and_hms_opt(0, 0, 0)
            .map_or(0, |t| t.and_utc().timestamp())
            + span * 86_400;
        i64::try_from(recorded_secs).is_ok_and(|recorded| recorded >= end + 40 * 86_400)
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn verify(zip: &[u8], checksum_file: &[u8], what: &str) -> VisionResult<()> {
    let text = String::from_utf8_lossy(checksum_file);
    let expected = text
        .split_whitespace()
        .next()
        .ok_or_else(|| VisionError::Checksum(format!("{what}: empty checksum file")))?
        .to_ascii_lowercase();
    let actual = sha256_hex(zip);
    if expected != actual {
        return Err(VisionError::Checksum(format!(
            "{what}: sha256 mismatch (expected {expected}, got {actual})"
        )));
    }
    Ok(())
}

fn write_atomic(path: &Path, bytes: &[u8]) -> VisionResult<()> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = append(
        path,
        &format!(
            ".tmp.{}.{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ),
    );
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Store `bytes` as a verified cache entry (zip plus matching checksum file).
pub fn cache_put(cache_dir: &Path, rel: &str, zip: &[u8]) -> VisionResult<()> {
    let path = cache_dir.join(rel);
    let name = rel.rsplit('/').next().unwrap_or(rel);
    write_atomic(
        &append(&path, ".CHECKSUM"),
        format!("{}  {name}\n", sha256_hex(zip)).as_bytes(),
    )?;
    write_atomic(&path, zip)
}

/// Content of the first file in a zip archive, size-bounded.
pub fn unzip_first(bytes: &[u8]) -> VisionResult<String> {
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes))?;
    if archive.is_empty() {
        return Err(VisionError::Parse("empty zip".into()));
    }
    let file = archive.by_index(0)?;
    let mut text = String::new();
    file.take(MAX_UNZIPPED_BYTES + 1)
        .read_to_string(&mut text)?;
    if u64::try_from(text.len()).unwrap_or(u64::MAX) > MAX_UNZIPPED_BYTES {
        return Err(VisionError::Parse("unzipped file too large".into()));
    }
    Ok(text)
}

impl Vision {
    pub fn new(cfg: VisionConfig) -> VisionResult<Self> {
        let client = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(Duration::from_secs(cfg.timeout_secs.max(1)))
            .build()?;
        Ok(Self {
            permits: tokio::sync::Semaphore::new(cfg.concurrency.max(1)),
            cfg,
            client,
            stats: Stats::default(),
        })
    }

    pub fn config(&self) -> &VisionConfig {
        &self.cfg
    }

    pub fn stats(&self) -> StatsSnapshot {
        let s = &self.stats;
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        StatsSnapshot {
            requests: get(&s.requests),
            cache_hits: get(&s.cache_hits),
            negative_cache_hits: get(&s.negative_hits),
            not_found_responses: get(&s.not_found),
            retries: get(&s.retries),
            offline_misses: get(&s.offline_misses),
            bytes_downloaded: get(&s.bytes_downloaded),
        }
    }

    fn charge(&self, bytes: u64) -> VisionResult<()> {
        let prev = self
            .stats
            .bytes_downloaded
            .fetch_add(bytes, Ordering::SeqCst);
        if prev.saturating_add(bytes) > self.cfg.max_download_bytes {
            self.stats
                .bytes_downloaded
                .fetch_sub(bytes, Ordering::SeqCst);
            return Err(VisionError::Budget {
                limit: self.cfg.max_download_bytes,
                needed: prev.saturating_add(bytes),
            });
        }
        Ok(())
    }

    /// Verified zip bytes from the cache, or `None` when there is no usable entry.
    fn read_cached(&self, rel: &str) -> VisionResult<Option<Vec<u8>>> {
        let path = self.cfg.cache_dir.join(rel);
        let zip = match std::fs::read(&path) {
            Ok(zip) => zip,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let checked = std::fs::read(append(&path, ".CHECKSUM"))
            .map_err(|e| VisionError::Checksum(format!("{rel}: checksum file unreadable: {e}")))
            .and_then(|sum| verify(&zip, &sum, rel));
        match checked {
            Ok(()) => Ok(Some(zip)),
            Err(e) if self.cfg.offline => Err(e),
            Err(_) => {
                // Corrupt or half-written entry: drop it and fetch again.
                let _ = std::fs::remove_file(&path);
                let _ = std::fs::remove_file(append(&path, ".CHECKSUM"));
                Ok(None)
            }
        }
    }

    /// Verified zip bytes, `None` if the archive does not have the file.
    pub async fn fetch_zip(&self, rel: &str) -> VisionResult<Option<Vec<u8>>> {
        if let Some(zip) = self.read_cached(rel)? {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(zip));
        }
        let negative = append(&self.cfg.cache_dir.join(rel), ".404");
        if let Ok(text) = std::fs::read_to_string(&negative) {
            let recorded = text.trim().parse::<u64>().unwrap_or(0);
            if self.cfg.offline || negative_valid(rel, recorded, now_secs()) {
                self.stats.negative_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
        }
        if self.cfg.offline {
            self.stats.offline_misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        let url = format!("{}/{rel}", self.cfg.base_url.trim_end_matches('/'));
        let Some(zip) = self.get(&url).await? else {
            write_atomic(&negative, now_secs().to_string().as_bytes())?;
            return Ok(None);
        };
        let Some(sum) = self.get(&format!("{url}.CHECKSUM")).await? else {
            return Err(VisionError::Checksum(format!(
                "{rel}: checksum file missing"
            )));
        };
        verify(&zip, &sum, rel)?;
        let path = self.cfg.cache_dir.join(rel);
        write_atomic(&append(&path, ".CHECKSUM"), &sum)?;
        write_atomic(&path, &zip)?;
        let _ = std::fs::remove_file(&negative);
        Ok(Some(zip))
    }

    /// CSV text of a dataset file, `None` if absent.
    pub async fn fetch_csv(&self, rel: &str) -> VisionResult<Option<String>> {
        match self.fetch_zip(rel).await? {
            Some(zip) => unzip_first(&zip).map(Some),
            None => Ok(None),
        }
    }

    /// GET with bounded concurrency, retries (5xx, timeouts, 429/Retry-After) and
    /// the download budget. `None` is a 404.
    async fn get(&self, url: &str) -> VisionResult<Option<Vec<u8>>> {
        let mut last = String::from("no attempt");
        for attempt in 0..self.cfg.max_attempts.max(1) {
            let backoff =
                Duration::from_millis(self.cfg.backoff_ms.saturating_mul(1 << attempt.min(8)));
            let mut wait = backoff;
            {
                let _permit = self
                    .permits
                    .acquire()
                    .await
                    .map_err(|e| VisionError::Http(e.to_string()))?;
                self.stats.requests.fetch_add(1, Ordering::Relaxed);
                match self.client.get(url).send().await {
                    Ok(resp) => {
                        let status = resp.status().as_u16();
                        match status {
                            200 => {
                                if let Some(len) = resp.content_length() {
                                    let used = self.stats.bytes_downloaded.load(Ordering::SeqCst);
                                    if used.saturating_add(len) > self.cfg.max_download_bytes {
                                        return Err(VisionError::Budget {
                                            limit: self.cfg.max_download_bytes,
                                            needed: used.saturating_add(len),
                                        });
                                    }
                                }
                                match resp.bytes().await {
                                    Ok(body) => {
                                        self.charge(u64::try_from(body.len()).unwrap_or(u64::MAX))?;
                                        return Ok(Some(body.to_vec()));
                                    }
                                    Err(e) => last = format!("{url}: body: {e}"),
                                }
                            }
                            404 => {
                                self.stats.not_found.fetch_add(1, Ordering::Relaxed);
                                return Ok(None);
                            }
                            429 | 500..=599 => {
                                last = format!("{url}: status {status}");
                                let retry_after = resp
                                    .headers()
                                    .get(reqwest::header::RETRY_AFTER)
                                    .and_then(|v| v.to_str().ok())
                                    .and_then(|v| v.trim().parse::<u64>().ok());
                                wait = match retry_after {
                                    Some(s) => Duration::from_secs(s.min(MAX_RETRY_AFTER_SECS)),
                                    None if status == 429 => backoff * 4,
                                    None => backoff,
                                };
                            }
                            _ => return Err(VisionError::Http(format!("{url}: status {status}"))),
                        }
                    }
                    Err(e) => last = format!("{url}: {e}"),
                }
            }
            self.stats.retries.fetch_add(1, Ordering::Relaxed);
            if attempt + 1 < self.cfg.max_attempts.max(1) {
                tokio::time::sleep(wait).await;
            }
        }
        Err(VisionError::Http(format!(
            "gave up after {} attempts: {last}",
            self.cfg.max_attempts
        )))
    }

    /// USDT-perpetual symbols with futures klines in the archive, delisted ones
    /// included. Offline, the last saved listing is used.
    pub async fn list_symbols(&self) -> VisionResult<Vec<String>> {
        let saved = self.cfg.cache_dir.join("_listing/um-symbols.txt");
        if self.cfg.offline {
            let text = std::fs::read_to_string(&saved).map_err(|e| {
                VisionError::Io(format!("offline listing {}: {e}", saved.display()))
            })?;
            return Ok(text.lines().map(str::to_owned).collect());
        }
        let prefix = "data/futures/um/monthly/klines/";
        let mut symbols = Vec::new();
        let mut marker = String::new();
        loop {
            let mut url = format!(
                "{}?prefix={prefix}&delimiter=/",
                self.cfg.listing_url.trim_end_matches('/')
            );
            if !marker.is_empty() {
                url.push_str(&format!("&marker={marker}"));
            }
            let body = self
                .get(&url)
                .await?
                .ok_or_else(|| VisionError::Http("listing: 404".into()))?;
            let page = String::from_utf8_lossy(&body).into_owned();
            let (found, next) = parse_listing(&page, prefix);
            symbols.extend(found);
            match next {
                Some(next) => marker = next,
                None => break,
            }
        }
        symbols.sort();
        symbols.dedup();
        write_atomic(&saved, symbols.join("\n").as_bytes())?;
        Ok(symbols)
    }
}

/// Tradable-looking perpetual names and the continuation marker of one S3 page.
pub fn parse_listing(xml: &str, prefix: &str) -> (Vec<String>, Option<String>) {
    let mut symbols = Vec::new();
    let mut last_prefix = None;
    let mut rest = xml;
    while let Some(start) = rest.find("<Prefix>") {
        let Some(after) = rest.get(start + "<Prefix>".len()..) else {
            break;
        };
        rest = after;
        let Some(end) = rest.find("</Prefix>") else {
            break;
        };
        let (Some(value), Some(after)) = (rest.get(..end), rest.get(end..)) else {
            break;
        };
        rest = after;
        if let Some(name) = value.strip_prefix(prefix).and_then(|n| n.strip_suffix('/')) {
            last_prefix = Some(value.to_owned());
            if name.ends_with("USDT")
                && !name.contains('_')
                && !name.contains("SETTLED")
                && name.chars().all(|c| c.is_ascii_alphanumeric())
            {
                symbols.push(name.to_owned());
            }
        }
    }
    let truncated = xml.contains("<IsTruncated>true</IsTruncated>");
    let next = if truncated {
        xml.split("<NextMarker>")
            .nth(1)
            .and_then(|t| t.split("</NextMarker>").next())
            .map(str::to_owned)
            .or(last_prefix)
    } else {
        None
    };
    (symbols, next)
}

// ---------------------------------------------------------------------------
// Parsers

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Kline {
    pub open_time_ms: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub base_volume: Decimal,
    pub quote_volume: Decimal,
    pub taker_buy_quote_volume: Decimal,
}

fn dec(field: &str) -> Option<Decimal> {
    let field = field.trim();
    Decimal::from_str(field)
        .or_else(|_| Decimal::from_scientific(field))
        .ok()
}

/// Epoch milliseconds; microsecond stamps (newer spot-style files) are converted.
fn epoch_ms(field: &str) -> Option<i64> {
    let value = field.trim().parse::<i64>().ok()?;
    Some(if value > 100_000_000_000_000 {
        value.div_euclid(1000)
    } else {
        value
    })
}

/// Header row optional. Rows that are malformed or not a valid candle are dropped;
/// the count of dropped rows is returned.
pub fn parse_klines(text: &str) -> (Vec<Kline>, usize) {
    let mut rows = Vec::new();
    let mut dropped = 0;
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let f: Vec<&str> = line.split(',').collect();
        if index == 0 && f.first().is_none_or(|v| v.trim().parse::<i64>().is_err()) {
            continue; // header
        }
        let parsed = (|| {
            if f.len() < 8 {
                return None;
            }
            let at = |i: usize| f.get(i).copied();
            let k = Kline {
                open_time_ms: epoch_ms(at(0)?)?,
                open: dec(at(1)?)?,
                high: dec(at(2)?)?,
                low: dec(at(3)?)?,
                close: dec(at(4)?)?,
                base_volume: dec(at(5)?)?,
                quote_volume: dec(at(7)?)?,
                taker_buy_quote_volume: f.get(10).and_then(|v| dec(v)).unwrap_or(Decimal::ZERO),
            };
            let valid = k.open_time_ms >= 0
                && k.low > Decimal::ZERO
                && k.high >= k.low
                && k.open >= k.low
                && k.open <= k.high
                && k.close >= k.low
                && k.close <= k.high
                && k.base_volume >= Decimal::ZERO
                && k.quote_volume >= Decimal::ZERO;
            valid.then_some(k)
        })();
        match parsed {
            Some(k) => rows.push(k),
            None => dropped += 1,
        }
    }
    rows.sort_by_key(|k| k.open_time_ms);
    rows.dedup_by_key(|k| k.open_time_ms);
    (rows, dropped)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricsRow {
    /// Row timestamp (`create_time`); the value is usable only after it.
    pub create_time_ms: i64,
    /// Open interest in base units.
    pub open_interest: Decimal,
}

pub fn parse_metrics(text: &str) -> (Vec<MetricsRow>, usize) {
    let mut lines = text.lines().filter(|l| !l.trim().is_empty()).peekable();
    let mut oi_col = 2;
    if let Some(first) = lines.peek() {
        if !first.starts_with(|c: char| c.is_ascii_digit()) {
            let header: Vec<&str> = first.split(',').map(str::trim).collect();
            oi_col = header
                .iter()
                .position(|h| *h == "sum_open_interest")
                .unwrap_or(2);
            lines.next();
        }
    }
    let (mut rows, mut dropped) = (Vec::new(), 0);
    for line in lines {
        let f: Vec<&str> = line.split(',').collect();
        let parsed = (|| {
            let time =
                NaiveDateTime::parse_from_str(f.first()?.trim(), "%Y-%m-%d %H:%M:%S").ok()?;
            let oi = dec(f.get(oi_col)?)?;
            (oi > Decimal::ZERO).then(|| MetricsRow {
                create_time_ms: time.and_utc().timestamp_millis(),
                open_interest: oi,
            })
        })();
        match parsed {
            Some(r) => rows.push(r),
            None => dropped += 1,
        }
    }
    rows.sort_by_key(|r| r.create_time_ms);
    rows.dedup_by_key(|r| r.create_time_ms);
    (rows, dropped)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FundingRow {
    /// Settlement time.
    pub calc_time_ms: i64,
    pub interval_hours: Option<u32>,
    pub rate: Decimal,
}

pub fn parse_funding(text: &str) -> (Vec<FundingRow>, usize) {
    let (mut rows, mut dropped) = (Vec::new(), 0);
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let f: Vec<&str> = line.split(',').map(str::trim).collect();
        if index == 0 && f.first().is_none_or(|v| v.parse::<i64>().is_err()) {
            continue;
        }
        let parsed = (|| {
            let calc_time_ms = epoch_ms(f.first()?)?;
            let rate = dec(f.last().filter(|_| f.len() >= 2)?)?;
            let interval_hours = if f.len() >= 3 {
                f.get(1)
                    .and_then(|v| v.parse::<u32>().ok())
                    .filter(|h| *h > 0)
            } else {
                None
            };
            Some(FundingRow {
                calc_time_ms,
                interval_hours,
                rate,
            })
        })();
        match parsed {
            Some(r) => rows.push(r),
            None => dropped += 1,
        }
    }
    rows.sort_by_key(|r| r.calc_time_ms);
    rows.dedup_by_key(|r| r.calc_time_ms);
    (rows, dropped)
}

/// `data/futures/um/{monthly|daily}/{dataset}[/{SYM}/{interval}]/…` path builders.
pub fn kline_path(symbol: &str, interval: &str, date: &str, daily: bool) -> String {
    let period = if daily { "daily" } else { "monthly" };
    format!("data/futures/um/{period}/klines/{symbol}/{interval}/{symbol}-{interval}-{date}.zip")
}
/// Spot monthly klines.
pub fn spot_kline_path(symbol: &str, interval: &str, month: &str) -> String {
    format!("data/spot/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{month}.zip")
}
pub fn metrics_path(symbol: &str, day: &str) -> String {
    format!("data/futures/um/daily/metrics/{symbol}/{symbol}-metrics-{day}.zip")
}
pub fn funding_path(symbol: &str, month: &str) -> String {
    format!("data/futures/um/monthly/fundingRate/{symbol}/{symbol}-fundingRate-{month}.zip")
}
/// USD-M monthly 1h premium-index klines.
pub fn premium_path(symbol: &str, month: &str) -> String {
    format!("data/futures/um/monthly/premiumIndexKlines/{symbol}/1h/{symbol}-1h-{month}.zip")
}
