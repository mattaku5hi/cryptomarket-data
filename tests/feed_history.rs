#![allow(clippy::unwrap_used, clippy::indexing_slicing)]

use market_data::feeds::{
    CandleInterval, HistoricalCandle, Instrument, Market, Venue, binance, bybit,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};

const MONDAY: i64 = 1_704_067_200_000; // 2024-01-01 00:00 UTC
const INTERVALS: [CandleInterval; 8] = [
    CandleInterval::M1,
    CandleInterval::M5,
    CandleInterval::M15,
    CandleInterval::M30,
    CandleInterval::H1,
    CandleInterval::H4,
    CandleInterval::D1,
    CandleInterval::W1,
];

fn instrument(venue: Venue, market: Market) -> Instrument {
    Instrument {
        venue,
        market,
        symbol: "BTCUSDT".into(),
    }
}

fn row(venue: Venue, start: i64, interval: CandleInterval) -> Value {
    match venue {
        Venue::Binance => json!([
            start,
            "100.000000000000000001",
            "102",
            "99",
            "101",
            "2.123456789012345678",
            start + interval.duration_ms() - 1,
            "212.345678901234567890",
            3,
            "1.000000000000000001",
            "100.000000000000000001",
            "0"
        ]),
        Venue::Bybit => json!([
            start.to_string(),
            "100.000000000000000001",
            "102",
            "99",
            "101",
            "2.123456789012345678",
            "212.345678901234567890"
        ]),
    }
}

fn payload(instrument: &Instrument, rows: Vec<Value>) -> Vec<u8> {
    let value = match instrument.venue {
        Venue::Binance => json!(rows),
        Venue::Bybit => json!({"retCode":0, "result":{
            "category":if instrument.market == Market::Spot { "spot" } else { "linear" },
            "symbol":instrument.symbol, "list":rows
        }}),
    };
    serde_json::to_vec(&value).unwrap()
}

fn parse(
    raw: &[u8],
    instrument: &Instrument,
    interval: CandleInterval,
    observed: i64,
) -> Result<Vec<HistoricalCandle>, market_data::feeds::ParseError> {
    match instrument.venue {
        Venue::Binance => binance::parse_rest_candles(raw, instrument, interval, observed),
        Venue::Bybit => bybit::parse_rest_candles(raw, instrument, interval, observed),
    }
}

#[test]
fn intervals_have_stable_provider_names_order_and_serialization() {
    let names = [
        ("1m", "1", 60_000),
        ("5m", "5", 300_000),
        ("15m", "15", 900_000),
        ("30m", "30", 1_800_000),
        ("1h", "60", 3_600_000),
        ("4h", "240", 14_400_000),
        ("1d", "D", 86_400_000),
        ("1w", "W", 604_800_000),
    ];
    for (interval, (binance, bybit, duration)) in INTERVALS.into_iter().zip(names) {
        assert_eq!(interval.binance_name(), binance);
        assert_eq!(interval.bybit_name(), bybit);
        assert_eq!(interval.duration_ms(), duration);
        let encoded = serde_json::to_string(&interval).unwrap();
        assert_eq!(
            serde_json::from_str::<CandleInterval>(&encoded).unwrap(),
            interval
        );
    }
    assert!(INTERVALS.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(
        serde_json::to_string(&CandleInterval::H1).unwrap(),
        "\"H1\""
    );
    assert!(serde_json::from_str::<CandleInterval>("\"1M\"").is_err());
}

#[test]
fn utc_bucket_boundaries_include_leap_day_and_monday_weeks() {
    for interval in INTERVALS {
        let duration = interval.duration_ms();
        assert_eq!(interval.bucket_start_ms(MONDAY - 1), MONDAY - duration);
        assert_eq!(interval.bucket_start_ms(MONDAY), MONDAY);
        assert_eq!(interval.bucket_start_ms(MONDAY + duration - 1), MONDAY);
        assert_eq!(
            interval.bucket_start_ms(MONDAY + duration),
            MONDAY + duration
        );
        let last_bucket = interval.bucket_start_ms(i64::MAX);
        assert_eq!(interval.bucket_start_ms(last_bucket), last_bucket);
        assert!(i64::MAX - last_bucket < duration);
        assert_eq!(interval.bucket_start_ms(i64::MIN), i64::MIN);
    }
    // Feb 29, 2024 is Thursday. Its week began Feb 26, not at an epoch multiple.
    let leap_day = 1_709_164_800_000;
    assert_eq!(CandleInterval::D1.bucket_start_ms(leap_day), leap_day);
    assert_eq!(
        CandleInterval::W1.bucket_start_ms(leap_day),
        1_708_905_600_000
    );
    assert_eq!(CandleInterval::W1.bucket_start_ms(0), -259_200_000);
    assert_eq!(CandleInterval::H1.bucket_start_ms(-1), -3_600_000);
}

#[test]
fn all_native_intervals_preserve_decimals_identity_and_exact_finality() {
    for venue in [Venue::Binance, Venue::Bybit] {
        for market in [Market::Spot, Market::UsdtPerpetual] {
            let instrument = instrument(venue, market);
            for interval in INTERVALS {
                let end = MONDAY + interval.duration_ms();
                let raw = payload(&instrument, vec![row(venue, MONDAY, interval)]);
                let forming = parse(&raw, &instrument, interval, end - 1).unwrap();
                assert!(!forming[0].is_final);
                let closed = parse(&raw, &instrument, interval, end).unwrap();
                let candle = &closed[0];
                assert!(candle.is_final);
                assert_eq!(candle.instrument, instrument);
                assert_eq!(candle.interval, interval);
                assert_eq!(candle.open_time_ms, MONDAY);
                assert_eq!(candle.updated_at_ms, end);
                assert_eq!(candle.open.to_string(), "100.000000000000000001");
                assert_eq!(candle.base_volume.to_string(), "2.123456789012345678");
                assert_eq!(candle.quote_volume.to_string(), "212.345678901234567890");
                assert_eq!(
                    candle.taker_buy_quote_volume,
                    (venue == Venue::Binance)
                        .then_some(Decimal::from_str_exact("100.000000000000000001").unwrap())
                );
                let encoded = serde_json::to_vec(candle).unwrap();
                assert_eq!(
                    serde_json::from_slice::<HistoricalCandle>(&encoded).unwrap(),
                    *candle
                );
            }
        }
    }
}

#[test]
fn native_history_rejects_misalignment_future_rows_duplicates_and_invalid_values() {
    for venue in [Venue::Binance, Venue::Bybit] {
        let instrument = instrument(venue, Market::UsdtPerpetual);
        for interval in [CandleInterval::H1, CandleInterval::D1, CandleInterval::W1] {
            let duration = interval.duration_ms();
            let aligned = row(venue, MONDAY, interval);
            let invalid_alignment =
                payload(&instrument, vec![row(venue, MONDAY + 60_000, interval)]);
            assert!(parse(&invalid_alignment, &instrument, interval, MONDAY + duration).is_err());
            let future = payload(&instrument, vec![aligned.clone()]);
            assert!(parse(&future, &instrument, interval, MONDAY - 1).is_err());
            let duplicate = payload(&instrument, vec![aligned.clone(), aligned.clone()]);
            assert!(parse(&duplicate, &instrument, interval, MONDAY + duration).is_err());
            let mut bad = row(venue, MONDAY + duration, interval);
            bad[2] = json!("99"); // High below open.
            let atomic = payload(&instrument, vec![aligned.clone(), bad]);
            assert!(parse(&atomic, &instrument, interval, MONDAY + duration).is_err());
            for (index, invalid) in [(5, json!("-1")), (1, json!(100.01)), (1, json!("1e2"))] {
                let mut bad = aligned.clone();
                bad[index] = invalid;
                assert!(
                    parse(
                        &payload(&instrument, vec![bad]),
                        &instrument,
                        interval,
                        MONDAY + duration
                    )
                    .is_err()
                );
            }
        }
    }
}

#[test]
fn binance_native_history_validates_inclusive_close_time_and_millisecond_contract() {
    let instrument = instrument(Venue::Binance, Market::Spot);
    for interval in [CandleInterval::H1, CandleInterval::D1, CandleInterval::W1] {
        let end = MONDAY + interval.duration_ms();
        for invalid_close in [end, end - 2, MONDAY + 59_999] {
            let mut bad = row(Venue::Binance, MONDAY, interval);
            bad[6] = json!(invalid_close);
            assert!(parse(&payload(&instrument, vec![bad]), &instrument, interval, end).is_err());
        }
        let mut microseconds = row(Venue::Binance, MONDAY, interval);
        microseconds[0] = json!(MONDAY * 1000);
        microseconds[6] = json!(end * 1000 - 1);
        assert!(
            parse(
                &payload(&instrument, vec![microseconds]),
                &instrument,
                interval,
                end * 1000
            )
            .is_err()
        );
    }
}

#[test]
fn native_history_preserves_provider_order_rules_and_m1_compatibility() {
    for venue in [Venue::Binance, Venue::Bybit] {
        let instrument = instrument(venue, Market::Spot);
        let interval = CandleInterval::H1;
        let end = MONDAY + interval.duration_ms();
        let rows = vec![row(venue, end, interval), row(venue, MONDAY, interval)];
        let reversed = parse(&payload(&instrument, rows), &instrument, interval, end);
        if venue == Venue::Bybit {
            let candles = reversed.unwrap();
            assert_eq!(candles[0].open_time_ms, MONDAY);
            assert!(candles[0].is_final);
            assert_eq!(candles[1].open_time_ms, end);
            assert!(!candles[1].is_final);
        } else {
            assert!(reversed.is_err());
        }
        let raw = payload(&instrument, vec![row(venue, MONDAY, CandleInterval::M1)]);
        let old = match venue {
            Venue::Binance => binance::parse_rest_klines(&raw, &instrument, end).unwrap(),
            Venue::Bybit => bybit::parse_rest_klines(&raw, &instrument, end).unwrap(),
        };
        let new = parse(&raw, &instrument, CandleInterval::M1, end).unwrap();
        let mut new_json = serde_json::to_value(&new[0]).unwrap();
        new_json.as_object_mut().unwrap().remove("interval");
        assert_eq!(new_json, serde_json::to_value(&old[0]).unwrap());
    }
}
