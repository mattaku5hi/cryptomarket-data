#![allow(clippy::unwrap_used, clippy::indexing_slicing)]
use market_data::feeds::{
    Instrument, Market, MarketEvent, ParseError, Side, Venue,
    bybit::{self, TickerCache},
};
use rust_decimal::Decimal;
use serde_json::{Value, json};

fn bytes(value: Value) -> Vec<u8> {
    serde_json::to_vec(&value).unwrap()
}
fn instrument(symbol: &str) -> Instrument {
    Instrument {
        venue: Venue::Bybit,
        market: Market::UsdtPerpetual,
        symbol: symbol.to_owned(),
    }
}
fn ticker(kind: &str, symbol: &str, time: i64, data: Value) -> Vec<u8> {
    let mut data = data;
    data["symbol"] = json!(symbol);
    bytes(
        json!({"topic":format!("tickers.{symbol}"), "type":kind, "cs":time, "ts":time, "data":data}),
    )
}
fn snapshot() -> Vec<u8> {
    ticker(
        "snapshot",
        "BTCUSDT",
        1672304486868,
        json!({
            "openInterest":"100.123456789012345678", "openInterestValue":"1657850.10",
            "fundingRate":"-0.0005", "markPrice":"16578.50", "indexPrice":"16580.25",
            "fundingIntervalHour":"8", "fundingCap":"0.005", "nextFundingTime":"1672329600000"
        }),
    )
}
fn candle(finalized: bool, volume: &str) -> Vec<u8> {
    bytes(
        json!({"topic":"kline.1.BTCUSDT", "type":"snapshot", "ts":1672324860000_i64,
        "data":[{"start":1672324800000_i64, "end":1672324859999_i64, "interval":"1",
            "open":"16649.5", "close":"16677", "high":"16677", "low":"16608",
            "volume":volume, "turnover":"34666.4005", "confirm":finalized,
            "timestamp":1672324858882_i64}]}),
    )
}

#[test]
fn ticker_preserves_missing_fields_and_emits_only_changed_metrics() {
    let mut cache = TickerCache::new(500).unwrap();
    let events = bybit::parse_websocket(&snapshot(), Market::UsdtPerpetual, &mut cache).unwrap();
    assert_eq!(events.len(), 2);
    let MarketEvent::Funding(funding) = &events[1] else {
        unreachable!()
    };
    assert_eq!(funding.rate, Decimal::new(-5, 4));
    assert_eq!(funding.interval_minutes, Some(480));
    assert_eq!(funding.next_funding_time_ms, Some(1672329600000));
    let before = cache.get(&instrument("BTCUSDT")).unwrap().clone();
    assert_eq!(
        before.open_interest.unwrap().to_string(),
        "100.123456789012345678"
    );
    let frame = ticker(
        "delta",
        "BTCUSDT",
        1672304486968,
        json!({"lastPrice":"16579.00"}),
    );
    assert!(
        bybit::parse_websocket(&frame, Market::UsdtPerpetual, &mut cache)
            .unwrap()
            .is_empty()
    );
    let after = cache.get(&instrument("BTCUSDT")).unwrap();
    assert_eq!(before.open_interest, after.open_interest);
    assert_eq!(before.funding_rate, after.funding_rate);
    let frame = ticker(
        "delta",
        "BTCUSDT",
        1672304487068,
        json!({"fundingRate":"0"}),
    );
    let events = bybit::parse_websocket(&frame, Market::UsdtPerpetual, &mut cache).unwrap();
    assert_eq!(events.len(), 1);
    let MarketEvent::Funding(funding) = &events[0] else {
        unreachable!()
    };
    assert_eq!(funding.rate, Decimal::ZERO);
    assert_eq!(funding.mark_price, before.mark_price);
}

#[test]
fn snapshot_resets_state_and_cache_is_per_symbol() {
    let mut cache = TickerCache::new(2).unwrap();
    cache.apply(&snapshot(), Market::UsdtPerpetual).unwrap();
    cache
        .apply(
            &ticker("snapshot", "ETHUSDT", 1, json!({"openInterest":"2"})),
            Market::UsdtPerpetual,
        )
        .unwrap();
    let state = cache
        .apply(
            &ticker("snapshot", "BTCUSDT", 2, json!({"fundingRate":"0.01"})),
            Market::UsdtPerpetual,
        )
        .unwrap();
    assert_eq!(state.open_interest, None);
    assert!(!state.open_interest_ready());
    assert!(state.funding_ready());
    assert_eq!(state.mark_price, None);
    assert_eq!(
        cache.get(&instrument("ETHUSDT")).unwrap().open_interest,
        Some(Decimal::new(2, 0))
    );
    assert!(matches!(
        cache.apply(
            &ticker("snapshot", "SOLUSDT", 3, json!({})),
            Market::UsdtPerpetual
        ),
        Err(ParseError::Limit(_))
    ));
    cache.clear();
    assert!(cache.get(&instrument("BTCUSDT")).is_none());
}

#[test]
fn delta_before_snapshot_is_explicitly_unready() {
    let mut cache = TickerCache::new(1).unwrap();
    let frame = ticker(
        "delta",
        "BTCUSDT",
        1,
        json!({"openInterest":"2", "fundingRate":"0.001"}),
    );
    assert!(
        bybit::parse_websocket(&frame, Market::UsdtPerpetual, &mut cache)
            .unwrap()
            .is_empty()
    );
    let state = cache.get(&instrument("BTCUSDT")).unwrap();
    assert!(!state.seen_snapshot);
    assert!(!state.open_interest_ready());
    assert!(!state.funding_ready());
    assert_eq!(state.open_interest, Some(Decimal::new(2, 0)));
    let frame = ticker("snapshot", "BTCUSDT", 2, json!({"openInterest":"3"}));
    cache.apply(&frame, Market::UsdtPerpetual).unwrap();
    assert_eq!(
        cache.get(&instrument("BTCUSDT")).unwrap().funding_rate,
        None
    );
}

#[test]
fn malformed_and_stale_patches_are_atomic() {
    let mut cache = TickerCache::new(1).unwrap();
    cache.apply(&snapshot(), Market::UsdtPerpetual).unwrap();
    let before = cache.get(&instrument("BTCUSDT")).unwrap().clone();
    for data in [
        json!({"openInterest":"9", "fundingRate":"NaN"}),
        json!({"openInterest":-1}),
        json!({"openInterest":"-1"}),
        json!({"fundingIntervalHour":"1.5"}),
        json!({"nextFundingTime":"-1"}),
        json!({"markPrice":"0.0000000000000000001"}),
    ] {
        let frame = ticker("delta", "BTCUSDT", 1672304486968, data);
        assert!(cache.apply(&frame, Market::UsdtPerpetual).is_err());
        assert_eq!(cache.get(&instrument("BTCUSDT")), Some(&before));
    }
    assert!(
        cache
            .apply(
                &ticker("delta", "BTCUSDT", 1, json!({"openInterest":"0"})),
                Market::UsdtPerpetual
            )
            .is_err()
    );
    assert_eq!(cache.get(&instrument("BTCUSDT")), Some(&before));
    let frame = ticker("delta", "BTCUSDT", 1672304486968, json!({"fundingRate":""}));
    assert_eq!(
        cache
            .apply(&frame, Market::UsdtPerpetual)
            .unwrap()
            .funding_rate,
        None
    );
}

#[test]
fn cumulative_candles_keep_latest_totals_and_finality() {
    let mut cache = TickerCache::new(1).unwrap();
    let first =
        bybit::parse_websocket(&candle(false, "2.081"), Market::UsdtPerpetual, &mut cache).unwrap();
    let second =
        bybit::parse_websocket(&candle(true, "2.100"), Market::UsdtPerpetual, &mut cache).unwrap();
    let MarketEvent::Candle(first) = &first[0] else {
        unreachable!()
    };
    let MarketEvent::Candle(second) = &second[0] else {
        unreachable!()
    };
    assert!(!first.is_final);
    assert!(second.is_final);
    assert_eq!(second.base_volume, Decimal::new(2100, 3));
    assert_eq!(second.taker_buy_quote_volume, None);
    assert_eq!(first.open_time_ms, second.open_time_ms);
    assert_eq!(second.quote_volume, Decimal::new(346664005, 4));
}

#[test]
fn malformed_candle_times_and_ohlc_are_rejected() {
    let mut cache = TickerCache::new(1).unwrap();
    for (field, val) in [
        ("start", json!(1672324800001_i64)),
        ("end", json!(1672324860000_i64)),
        ("low", json!("17000")),
        ("volume", json!("-1")),
        ("interval", json!("5")),
        ("timestamp", json!(1672324860001_i64)),
    ] {
        let mut frame: Value = serde_json::from_slice(&candle(true, "2")).unwrap();
        frame["data"][0][field] = val;
        assert!(bybit::parse_websocket(&bytes(frame), Market::UsdtPerpetual, &mut cache).is_err());
    }
}

#[test]
fn trades_preserve_ids_taker_side_and_exact_quantity() {
    let frame = bytes(
        json!({"topic":"publicTrade.ETHUSDT","type":"snapshot","ts":1672304486868_i64,
        "data":[{"T":1672304486865_i64,"s":"ETHUSDT","S":"Buy","v":"0.001","p":"1657.85",
        "i":"20f43950-d8dd-5b31-9112-a178eb6023af","seq":123},
        {"T":1672304486865_i64,"s":"ETHUSDT","S":"Sell","v":"0.002","p":"1657.84","i":"trade-2","seq":123}]}),
    );
    let mut cache = TickerCache::new(1).unwrap();
    let events = bybit::parse_websocket(&frame, Market::Spot, &mut cache).unwrap();
    assert_eq!(events.len(), 2);
    let MarketEvent::Trade(buy) = &events[0] else {
        unreachable!()
    };
    let MarketEvent::Trade(sell) = &events[1] else {
        unreachable!()
    };
    assert_eq!(buy.side, Side::Buy);
    assert_eq!(sell.side, Side::Sell);
    assert_eq!(buy.quantity, Decimal::new(1, 3));
    assert_eq!(buy.instrument.market, Market::Spot);
    assert_eq!(sell.id, "trade-2");
    // Repeated sequence values do not cause trade loss or ID rewriting.
    assert_eq!(
        events,
        bybit::parse_websocket(&frame, Market::Spot, &mut cache).unwrap()
    );
    let mut bad: Value = serde_json::from_slice(&frame).unwrap();
    bad["data"][1]["s"] = json!("BTCUSDT");
    assert_eq!(
        bybit::parse_websocket(&bytes(bad), Market::Spot, &mut cache),
        Err(ParseError::InstrumentMismatch)
    );
}

#[test]
fn rest_klines_sort_and_preserve_forming_rows_without_taker_estimates() {
    let frame = bytes(
        json!({"retCode":0,"result":{"category":"linear","symbol":"BTCUSDT","list":[
        ["1672324860000","10","12","9","11","2","21"],
        ["1672324800000","10","12","9","11","3","31"]]}}),
    );
    let candles = bybit::parse_rest_klines(&frame, &instrument("BTCUSDT"), 1672324861000).unwrap();
    assert_eq!(candles[0].open_time_ms, 1672324800000);
    assert!(candles[0].is_final);
    assert!(!candles[1].is_final);
    assert_eq!(candles[0].taker_buy_quote_volume, None);
    assert_eq!(candles[0].quote_volume, Decimal::new(31, 0));
    assert!(bybit::parse_rest_klines(&frame, &instrument("ETHUSDT"), 1672324861000).is_err());
}

#[test]
fn metadata_exposes_pagination_filters_non_usdt_perpetuals_and_preserves_eligibility() {
    let frame = bytes(
        json!({"retCode":0,"result":{"category":"linear","nextPageCursor":"BTCUSDT%2CETHUSDT",
        "list":[{"symbol":"BTCUSDT","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT","contractType":"LinearPerpetual",
            "status":"Trading","launchTime":"1585526400000","deliveryTime":"0","leverageFilter":{"maxLeverage":"100.00"}},
        {"symbol":"ETHUSDT","baseCoin":"ETH","quoteCoin":"USDT","settleCoin":"USDT","contractType":"LinearPerpetual",
            "status":"PreLaunch","launchTime":"1685526400000","deliveryTime":"1785526400000","leverageFilter":{"maxLeverage":"20"}},
        {"symbol":"BTC-26DEC25","baseCoin":"BTC","quoteCoin":"USDT","contractType":"LinearFutures"},
        {"symbol":"BTCPERP","baseCoin":"BTC","quoteCoin":"USDC","contractType":"LinearPerpetual"}]}}),
    );
    let page = bybit::parse_instruments(&frame, Market::UsdtPerpetual).unwrap();
    assert_eq!(page.instruments.len(), 2);
    assert_eq!(page.next_cursor.as_deref(), Some("BTCUSDT%2CETHUSDT"));
    assert_eq!(page.instruments[0].max_leverage, Some(Decimal::new(100, 0)));
    assert_eq!(page.instruments[0].delivery_time_ms, None);
    assert_eq!(page.instruments[1].status, "PreLaunch");
    assert_eq!(page.instruments[1].delivery_time_ms, Some(1785526400000));
    let spot = bytes(json!({"retCode":0,"result":{"category":"spot","list":[{
        "symbol":"BTCUSDT","baseCoin":"BTC","quoteCoin":"USDT","status":"Trading"}]}}));
    let page = bybit::parse_instruments(&spot, Market::Spot).unwrap();
    assert_eq!(page.next_cursor, None);
    assert!(!page.instruments[0].is_perpetual);
    assert_eq!(page.instruments[0].listing_time_ms, None);
}

#[test]
fn oi_retains_base_units_and_cursor() {
    let frame = bytes(
        json!({"retCode":0,"result":{"category":"linear","symbol":"BTCUSDT","nextPageCursor":"cursor1",
        "list":[{"openInterest":"14592.562","timestamp":"1669571400000"},
        {"openInterest":"14500.123","timestamp":"1669571100000"}]}}),
    );
    let page = bybit::parse_open_interest(&frame, &instrument("BTCUSDT")).unwrap();
    assert_eq!(page.observations[0].value, Decimal::new(14500123, 3));
    assert_eq!(page.observations[1].time_ms, 1669571400000);
    assert_eq!(page.next_cursor.as_deref(), Some("cursor1"));
}

#[test]
fn rejects_bad_envelopes_duplicate_keys_bad_symbols_and_oversized_frames() {
    let mut cache = TickerCache::new(1).unwrap();
    assert_eq!(
        cache.apply(
            br#"{"topic":"tickers.BTCUSDT","topic":"tickers.ETHUSDT"}"#,
            Market::UsdtPerpetual
        ),
        Err(ParseError::MalformedJson)
    );
    assert!(
        cache
            .apply(
                &ticker("snapshot", "btcUSDT", 1, json!({})),
                Market::UsdtPerpetual
            )
            .is_err()
    );
    assert!(
        cache
            .apply(&ticker("delta", "BTCUSDT", 1, json!({})), Market::Spot)
            .is_err()
    );
    assert!(TickerCache::new(0).is_err());
    assert_eq!(
        cache.apply(&vec![b' '; 8 * 1024 * 1024 + 1], Market::Spot),
        Err(ParseError::Limit("frame bytes"))
    );
    assert!(bybit::parse_instruments(br#"{"retCode":10001,"result":{}}"#, Market::Spot).is_err());
    let frame =
        bytes(json!({"retCode":0,"result":{"category":"spot","nextPageCursor":"oops","list":[]}}));
    assert!(bybit::parse_instruments(&frame, Market::Spot).is_err());
}

#[test]
fn universe_turnover_rest_and_ws_preserve_quote_units() {
    let frame = bytes(json!({"retCode":0,"result":{"category":"linear","list":[
        {"symbol":"BTCUSDT","turnover24h":"141946527.22907118"},
        {"symbol":"BTC-26DEC25","turnover24h":"1"},
        {"symbol":"BTCPERP","turnover24h":"2"}]}}));
    let turnovers = bybit::parse_turnovers(&frame, Market::UsdtPerpetual).unwrap();
    assert_eq!(turnovers.len(), 1);
    assert_eq!(
        turnovers[&instrument("BTCUSDT")].to_string(),
        "141946527.22907118"
    );
    let mut cache = TickerCache::new(1).unwrap();
    let frame = ticker(
        "snapshot",
        "BTCUSDT",
        1,
        json!({"turnover24h":"141946527.22907118"}),
    );
    let state = cache.apply(&frame, Market::Spot).unwrap();
    assert_eq!(state.turnover_24h, Some(turnovers[&instrument("BTCUSDT")]));
    assert_eq!(state.open_interest, None);
    let frame = ticker("snapshot", "BTCUSDT", 2, json!({"lastPrice":"10"}));
    assert_eq!(
        cache.apply(&frame, Market::Spot).unwrap().turnover_24h,
        None
    );
}

#[test]
fn linear_metadata_requires_usdt_settlement_without_imposing_it_on_spot() {
    let mut row = json!({"symbol":"BTCUSDT","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDC","contractType":"LinearPerpetual",
        "status":"Trading","launchTime":"1585526400000","deliveryTime":"0","leverageFilter":{"maxLeverage":"100"}});
    let frame = bytes(json!({"retCode":0,"result":{"category":"linear","list":[row]}}));
    assert!(
        bybit::parse_instruments(&frame, Market::UsdtPerpetual)
            .unwrap()
            .instruments
            .is_empty()
    );
    row.as_object_mut().unwrap().remove("settleCoin");
    let frame = bytes(json!({"retCode":0,"result":{"category":"linear","list":[row]}}));
    assert!(bybit::parse_instruments(&frame, Market::UsdtPerpetual).is_err());
    let frame = bytes(
        json!({"retCode":0,"result":{"category":"spot","list":[{"symbol":"BTCUSDT","baseCoin":"BTC","quoteCoin":"USDT","status":"Trading"}]}}),
    );
    assert_eq!(
        bybit::parse_instruments(&frame, Market::Spot)
            .unwrap()
            .instruments
            .len(),
        1
    );
}
