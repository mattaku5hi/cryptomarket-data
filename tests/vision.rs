#![cfg(feature = "vision")]
#![allow(clippy::unwrap_used, clippy::indexing_slicing)]
//! Offline tests of the Binance Vision archive client (against a local mock server)
//! and its CSV parsers.

use std::{
    io::{Cursor, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use market_data::vision::{
    Vision, VisionConfig, VisionError, cache_put, funding_path, kline_path, metrics_path,
    parse_funding, parse_klines, parse_listing, parse_metrics, premium_path, spot_kline_path,
};
use rust_decimal::Decimal;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const M: i64 = 60_000;

/// Unique directory under the system temp dir, removed on drop.
struct TempDir(PathBuf);
impl TempDir {
    fn new() -> std::io::Result<Self> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "market-data-vision-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path)?;
        Ok(Self(path))
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn dec(s: &str) -> Decimal {
    s.parse().unwrap()
}

fn zip_bytes(name: &str, content: &str) -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    writer
        .start_file(name, zip::write::SimpleFileOptions::default())
        .unwrap();
    writer.write_all(content.as_bytes()).unwrap();
    writer.finish().unwrap().into_inner()
}

fn ms(year: i32, month: u32, day: u32, hour: u32) -> i64 {
    chrono::NaiveDate::from_ymd_opt(year, month, day)
        .unwrap()
        .and_hms_opt(hour, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis()
}

// ---------------------------------------------------------------------------
// Parsing

#[test]
fn klines_parse_with_and_without_header_and_microseconds() {
    let header = "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n";
    let row = "1700000000000,1.0,2.0,0.5,1.5,10,1700000059999,15.5,7,4,6.2,0\n";
    let (with, dropped) = parse_klines(&format!("{header}{row}"));
    let (without, _) = parse_klines(row);
    assert_eq!(dropped, 0);
    assert_eq!(with, without);
    assert_eq!(with[0].open_time_ms, 1_700_000_000_000);
    assert_eq!(with[0].quote_volume, dec("15.5"));
    assert_eq!(with[0].taker_buy_quote_volume, dec("6.2"));

    let micro = "1700000000000000,1.0,2.0,0.5,1.5,10,1700000059999999,15.5,7,4,6.2,0\n";
    let (rows, _) = parse_klines(micro);
    assert_eq!(rows[0].open_time_ms, 1_700_000_000_000);

    // Malformed and invalid rows are dropped and counted; order and duplicates are fixed.
    let text = format!(
        "{row}garbage\n1700000060000,1,0.5,2,1,1,0,1,1,1,1,0\n1699999940000,1.0,2.0,0.5,1.5,10,0,15.5,7,4,6.2,0\n{row}"
    );
    let (rows, dropped) = parse_klines(&text);
    assert_eq!(dropped, 2);
    assert_eq!(rows.len(), 2);
    assert!(rows[0].open_time_ms < rows[1].open_time_ms);
}

#[test]
fn metrics_and_funding_parse() {
    let metrics = "create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio\n\
                   2026-03-10 00:05:00,AAAUSDT,1234.5,99999,1.1\n\
                   2026-03-10 00:00:00,AAAUSDT,1200,99999,1.1\n\
                   bad line\n";
    let (rows, dropped) = parse_metrics(metrics);
    assert_eq!(dropped, 1);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].create_time_ms, ms(2026, 3, 10, 0));
    assert_eq!(rows[1].create_time_ms, ms(2026, 3, 10, 0) + 5 * M);
    assert_eq!(rows[1].open_interest, dec("1234.5"));

    let funding = "calc_time,funding_interval_hours,last_funding_rate\n1772582400000,8,0.00010000\n1772611200000,4,-0.00020000\n";
    let (rows, dropped) = parse_funding(funding);
    assert_eq!(dropped, 0);
    assert_eq!(rows[0].interval_hours, Some(8));
    assert_eq!(rows[0].rate, dec("0.0001"));
    assert_eq!(rows[1].interval_hours, Some(4));
    assert_eq!(rows[1].rate, dec("-0.0002"));
    let (rows, _) = parse_funding("1772582400000,0.0001\n");
    assert_eq!(rows[0].interval_hours, None);
}

#[test]
fn listing_filters_and_paginates() {
    let prefix = "data/futures/um/monthly/klines/";
    let page = |names: &[&str], truncated: bool| {
        let items: String = names
            .iter()
            .map(|n| format!("<CommonPrefixes><Prefix>{prefix}{n}/</Prefix></CommonPrefixes>"))
            .collect();
        format!(
            "<ListBucketResult><IsTruncated>{truncated}</IsTruncated>{}{items}</ListBucketResult>",
            if truncated {
                format!(
                    "<NextMarker>{prefix}{}/</NextMarker>",
                    names.last().unwrap()
                )
            } else {
                String::new()
            }
        )
    };
    let (symbols, next) = parse_listing(
        &page(
            &[
                "BTCUSDT",
                "BTCUSDT_250627",
                "ETHUSD_PERP",
                "FOOUSDT",
                "BTCSETTLED",
                "BARBUSD",
            ],
            true,
        ),
        prefix,
    );
    assert_eq!(symbols, vec!["BTCUSDT", "FOOUSDT"]);
    assert_eq!(
        next.as_deref(),
        Some("data/futures/um/monthly/klines/BARBUSD/")
    );
    let (symbols, next) = parse_listing(&page(&["ZZZUSDT"], false), prefix);
    assert_eq!(symbols, vec!["ZZZUSDT"]);
    assert!(next.is_none());
}

// ---------------------------------------------------------------------------
// Mock archive server

struct Reply {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
}
fn reply(status: u16, body: Vec<u8>) -> Reply {
    Reply {
        status,
        headers: Vec::new(),
        body,
    }
}

type Handler = Arc<dyn Fn(&str, usize) -> Reply + Send + Sync>;

/// Local HTTP server; the handler gets the request target and how many times it
/// has been requested before (zero for the first request).
async fn serve(handler: Handler) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let log: Arc<Mutex<Vec<String>>> = Arc::default();
    let seen = log.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let (handler, seen) = (handler.clone(), seen.clone());
            tokio::spawn(async move {
                let mut buffer = Vec::new();
                let mut chunk = [0u8; 1024];
                while !buffer.windows(4).any(|w| w == b"\r\n\r\n") {
                    match socket.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buffer.extend_from_slice(&chunk[..n]),
                    }
                }
                let text = String::from_utf8_lossy(&buffer).into_owned();
                let target = text.split_whitespace().nth(1).unwrap_or("/").to_owned();
                let before = {
                    let mut log = seen.lock().unwrap();
                    let before = log.iter().filter(|t| **t == target).count();
                    log.push(target.clone());
                    before
                };
                let reply = handler(&target, before);
                let mut head = format!(
                    "HTTP/1.1 {} X\r\nContent-Length: {}\r\nConnection: close\r\n",
                    reply.status,
                    reply.body.len()
                );
                for (k, v) in &reply.headers {
                    head.push_str(&format!("{k}: {v}\r\n"));
                }
                head.push_str("\r\n");
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(&reply.body).await;
                let _ = socket.shutdown().await;
            });
        }
    });
    (base, log)
}

fn vision_for(base: &str, cache: &Path) -> Vision {
    let mut cfg = VisionConfig::new(cache);
    cfg.base_url = base.into();
    cfg.listing_url = base.into();
    cfg.backoff_ms = 1;
    Vision::new(cfg).unwrap()
}

fn hex(zip: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(zip)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

const OLD_FILE: &str = "data/futures/um/monthly/klines/OLDUSDT/1h/OLDUSDT-1h-2020-01.zip";

#[tokio::test]
async fn missing_files_are_negatively_cached() {
    let cache = TempDir::new().unwrap();
    let (base, log) = serve(Arc::new(|_, _| reply(404, b"nope".to_vec()))).await;
    let vision = vision_for(&base, cache.path());
    assert!(vision.fetch_csv(OLD_FILE).await.unwrap().is_none());
    assert!(vision.fetch_csv(OLD_FILE).await.unwrap().is_none());
    assert_eq!(
        log.lock().unwrap().len(),
        1,
        "second lookup must not hit the network"
    );
    let stats = vision.stats();
    assert_eq!((stats.requests, stats.negative_cache_hits), (1, 1));

    // An offline run honours the entry and never errors.
    let mut cfg = VisionConfig::new(cache.path());
    cfg.offline = true;
    let offline = Vision::new(cfg).unwrap();
    assert!(offline.fetch_csv(OLD_FILE).await.unwrap().is_none());
    assert_eq!(offline.stats().requests, 0);
}

#[tokio::test]
async fn verified_download_is_cached_and_reused() {
    let cache = TempDir::new().unwrap();
    let zip = zip_bytes("x.csv", "a,b\n1,2\n");
    let sum = format!("{}  OLDUSDT-1h-2020-01.zip\n", hex(&zip));
    let (zip_c, sum_len, sum) = (zip.clone(), sum.len(), sum);
    let (base, log) = serve(Arc::new(move |target, _| {
        if target.ends_with(".CHECKSUM") {
            reply(200, sum.clone().into_bytes())
        } else {
            reply(200, zip_c.clone())
        }
    }))
    .await;
    let vision = vision_for(&base, cache.path());
    assert_eq!(
        vision.fetch_csv(OLD_FILE).await.unwrap().unwrap(),
        "a,b\n1,2\n"
    );
    assert_eq!(
        vision.fetch_csv(OLD_FILE).await.unwrap().unwrap(),
        "a,b\n1,2\n"
    );
    assert_eq!(log.lock().unwrap().len(), 2, "zip and checksum once");
    let stats = vision.stats();
    assert_eq!(stats.cache_hits, 1);
    assert_eq!(
        usize::try_from(stats.bytes_downloaded).unwrap(),
        zip.len() + sum_len
    );
    assert!(cache.path().join(OLD_FILE).exists());
    assert!(cache.path().join(format!("{OLD_FILE}.CHECKSUM")).exists());
}

#[tokio::test]
async fn checksum_mismatch_is_rejected_and_not_cached() {
    let cache = TempDir::new().unwrap();
    let zip = zip_bytes("x.csv", "a\n1\n");
    let (base, _) = serve(Arc::new(move |target, _| {
        if target.ends_with(".CHECKSUM") {
            reply(200, format!("{}  f.zip\n", "0".repeat(64)).into_bytes())
        } else {
            reply(200, zip.clone())
        }
    }))
    .await;
    let vision = vision_for(&base, cache.path());
    let error = vision.fetch_csv(OLD_FILE).await.unwrap_err();
    assert!(matches!(error, VisionError::Checksum(_)), "{error}");
    assert!(!cache.path().join(OLD_FILE).exists());
}

#[tokio::test]
async fn corrupt_cache_entry_is_an_error_offline() {
    let cache = TempDir::new().unwrap();
    cache_put(cache.path(), OLD_FILE, &zip_bytes("x.csv", "a\n1\n")).unwrap();
    let mut cfg = VisionConfig::new(cache.path());
    cfg.offline = true;
    let vision = Vision::new(cfg.clone()).unwrap();
    assert!(vision.fetch_csv(OLD_FILE).await.unwrap().is_some());
    std::fs::write(
        cache.path().join(OLD_FILE),
        zip_bytes("x.csv", "tampered\n"),
    )
    .unwrap();
    let error = Vision::new(cfg)
        .unwrap()
        .fetch_csv(OLD_FILE)
        .await
        .unwrap_err();
    assert!(matches!(error, VisionError::Checksum(_)), "{error}");
}

#[tokio::test]
async fn retries_server_errors_and_honours_retry_after() {
    let cache = TempDir::new().unwrap();
    let zip = zip_bytes("x.csv", "a\n1\n");
    let sum = format!("{}  f.zip\n", hex(&zip));
    let (base, _) = serve(Arc::new(move |target, before| {
        if target.ends_with(".CHECKSUM") {
            return reply(200, sum.clone().into_bytes());
        }
        match before {
            0 => reply(503, Vec::new()),
            1 => Reply {
                status: 429,
                headers: vec![("Retry-After", "0".into())],
                body: Vec::new(),
            },
            _ => reply(200, zip.clone()),
        }
    }))
    .await;
    let vision = vision_for(&base, cache.path());
    assert!(vision.fetch_csv(OLD_FILE).await.unwrap().is_some());
    let stats = vision.stats();
    assert_eq!(stats.retries, 2);
    assert_eq!(stats.requests, 4);
}

#[tokio::test]
async fn gives_up_after_max_attempts() {
    let cache = TempDir::new().unwrap();
    let (base, log) = serve(Arc::new(|_, _| reply(500, Vec::new()))).await;
    let vision = vision_for(&base, cache.path());
    assert!(matches!(
        vision.fetch_csv(OLD_FILE).await,
        Err(VisionError::Http(_))
    ));
    assert_eq!(log.lock().unwrap().len(), 5);
}

#[tokio::test]
async fn download_budget_is_enforced() {
    let cache = TempDir::new().unwrap();
    let (base, _) = serve(Arc::new(|_, _| reply(200, vec![7u8; 5000]))).await;
    let mut cfg = VisionConfig::new(cache.path());
    cfg.base_url = base;
    cfg.max_download_bytes = 1000;
    let vision = Vision::new(cfg).unwrap();
    let error = vision.fetch_csv(OLD_FILE).await.unwrap_err();
    assert!(
        matches!(error, VisionError::Budget { limit: 1000, .. }),
        "{error}"
    );
    assert_eq!(vision.stats().bytes_downloaded, 0);
    assert!(!cache.path().join(OLD_FILE).exists());
}

#[tokio::test]
async fn listing_follows_markers_and_is_saved_for_offline_use() {
    let cache = TempDir::new().unwrap();
    let prefix = "data/futures/um/monthly/klines/";
    let (base, _) = serve(Arc::new(move |target, _| {
        let body = if target.contains("marker=") {
            format!("<R><IsTruncated>false</IsTruncated><CommonPrefixes><Prefix>{prefix}BBBUSDT/</Prefix></CommonPrefixes></R>")
        } else {
            format!("<R><IsTruncated>true</IsTruncated><NextMarker>{prefix}AAAUSDT/</NextMarker><CommonPrefixes><Prefix>{prefix}AAAUSDT/</Prefix></CommonPrefixes></R>")
        };
        reply(200, body.into_bytes())
    }))
    .await;
    let vision = vision_for(&base, cache.path());
    assert_eq!(
        vision.list_symbols().await.unwrap(),
        vec!["AAAUSDT", "BBBUSDT"]
    );
    let mut cfg = VisionConfig::new(cache.path());
    cfg.offline = true;
    assert_eq!(
        Vision::new(cfg).unwrap().list_symbols().await.unwrap(),
        vec!["AAAUSDT", "BBBUSDT"]
    );
}

#[test]
fn path_builders_mirror_the_archive_layout() {
    assert_eq!(
        kline_path("BTCUSDT", "1h", "2026-08", false),
        "data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2026-08.zip"
    );
    assert_eq!(
        kline_path("BTCUSDT", "1m", "2026-08-01", true),
        "data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2026-08-01.zip"
    );
    assert_eq!(
        spot_kline_path("BTCUSDT", "1h", "2026-08"),
        "data/spot/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2026-08.zip"
    );
    assert_eq!(
        metrics_path("BTCUSDT", "2026-08-01"),
        "data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2026-08-01.zip"
    );
    assert_eq!(
        funding_path("BTCUSDT", "2026-08"),
        "data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-08.zip"
    );
    assert_eq!(
        premium_path("BTCUSDT", "2026-08"),
        "data/futures/um/monthly/premiumIndexKlines/BTCUSDT/1h/BTCUSDT-1h-2026-08.zip"
    );
}
