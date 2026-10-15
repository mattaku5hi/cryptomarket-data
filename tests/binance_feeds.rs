#![allow(clippy::unwrap_used, clippy::indexing_slicing)]
use market_data::feeds::{Instrument, Market, MarketEvent, ParseError, Side, Venue, binance::*};
use rust_decimal::Decimal;
use serde_json::{Value, json};

fn instrument(market: Market) -> Instrument {
    Instrument {
        venue: Venue::Binance,
        market,
        symbol: "BTCUSDT".into(),
    }
}
fn candle() -> Value {
    json!({"e":"kline","E":1638747719999_i64,"s":"BTCUSDT","k":{
        "t":1638747660000_i64,"T":1638747719999_i64,"s":"BTCUSDT","i":"1m",
        "o":"60000.01","h":"60010.50","l":"59990.10","c":"60001.25",
        "v":"1.250","q":"75000.12345678","V":"0.5","Q":"30000.01000001","x":true
    }})
}
fn mark(symbol: &str) -> Value {
    json!({"e":"markPriceUpdate","E":1562305380000_i64,"s":symbol,
        "p":"11185.87786614","i":"11784.62659091","r":"-0.00030000","T":1562306400000_i64,"st":1})
}
fn raw(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

#[test]
fn raw_and_combined_candles_preserve_exact_quote_volume_and_identity() {
    for market in [Market::Spot, Market::UsdtPerpetual] {
        let expected = parse_websocket(&raw(&candle()), market).unwrap();
        let combined = json!({"stream":"btcusdt@kline_1m","data":candle()});
        assert_eq!(parse_websocket(&raw(&combined), market).unwrap(), expected);
        let MarketEvent::Candle(c) = &expected[0] else {
            unreachable!()
        };
        assert_eq!(c.instrument, instrument(market));
        assert_eq!(c.quote_volume.to_string(), "75000.12345678");
        assert_eq!(
            c.taker_buy_quote_volume.unwrap().to_string(),
            "30000.01000001"
        );
        assert!(c.is_final);
    }
}
#[test]
fn kline_rejects_bad_identity_interval_time_ohlc_and_inexact_numbers() {
    for (path, value) in [
        ("s", json!("ETHUSDT")),
        ("i", json!("5m")),
        ("t", json!(-1)),
        ("T", json!(1638747959999_i64)),
        ("o", json!("0")),
        ("l", json!("60001")),
        ("q", json!(75000.12345678)),
        ("q", json!("1e5")),
        ("q", json!("01.2")),
        ("q", json!("1.0000000000000000001")),
        ("Q", json!("999999")),
        ("v", json!("-1")),
    ] {
        let mut input = candle();
        input["k"][path] = value;
        assert!(
            parse_websocket(&raw(&input), Market::Spot).is_err(),
            "{path}"
        );
    }
    let bad_stream = json!({"stream":"ethusdt@kline_1m","data":candle()});
    assert_eq!(
        parse_websocket(&raw(&bad_stream), Market::Spot),
        Err(ParseError::InstrumentMismatch)
    );
    let mut microseconds = candle();
    microseconds["E"] = json!(1638747719999000_i64);
    assert!(parse_websocket(&raw(&microseconds), Market::Spot).is_err());
}
#[test]
fn all_market_funding_is_atomic_and_filters_other_contract_units() {
    let mut coin_m = mark("BTCUSD_PERP");
    coin_m["st"] = json!(2);
    let rows = json!([mark("BTCUSDT"), mark("ETHUSDT"), mark("BTCUSDC"), coin_m]);
    let events = parse_websocket(
        &raw(&json!({"stream":"!markPrice@arr@1s","data":rows})),
        Market::UsdtPerpetual,
    )
    .unwrap();
    assert_eq!(events.len(), 2);
    let MarketEvent::Funding(funding) = &events[0] else {
        unreachable!()
    };
    assert_eq!(funding.rate, Decimal::new(-3, 4));
    assert_eq!(funding.interval_minutes, None);
    assert_eq!(funding.instrument.symbol, "BTCUSDT");
    let invalid = json!([mark("BTCUSDT"), {"e":"markPriceUpdate","s":"ETHUSDT"}]);
    assert!(parse_websocket(&raw(&invalid), Market::UsdtPerpetual).is_err());
    assert!(parse_websocket(&raw(&rows), Market::Spot).is_err());
}
#[test]
fn liquidation_uses_filled_quantity_average_price_and_executed_side() {
    let payload = json!({"e":"forceOrder","E":1568014460893_i64,"st":1,"ps":"BTCUSDT","o":{
        "s":"BTCUSDT","S":"SELL","q":"2.0","p":"9910","ap":"9908.5","z":"1.5","l":"0.5","T":1568014460893_i64
    }});
    let events = parse_websocket(
        &raw(&json!({"stream":"!forceOrder@arr","data":payload})),
        Market::UsdtPerpetual,
    )
    .unwrap();
    let MarketEvent::Liquidation(l) = &events[0] else {
        unreachable!()
    };
    assert_eq!(l.side, Side::Sell);
    assert_eq!(l.quantity, Decimal::new(15, 1));
    assert_eq!(l.price, Decimal::new(99085, 1));
    assert_eq!(l.id, None);
    let mut invalid = payload.clone();
    invalid["o"]["ap"] = json!("0");
    assert!(parse_websocket(&raw(&invalid), Market::UsdtPerpetual).is_err());
    invalid = payload.clone();
    invalid["o"]["z"] = json!("3");
    assert!(parse_websocket(&raw(&invalid), Market::UsdtPerpetual).is_err());
}
#[test]
fn rest_klines_track_partial_finality_and_reject_reordering() {
    let row = json!([
        1638747660000_i64,
        "60000",
        "60010",
        "59990",
        "60001",
        "1",
        1638747719999_i64,
        "60000",
        2,
        "0.5",
        "30000",
        "0"
    ]);
    let rows = json!([row]);
    let i = instrument(Market::Spot);
    let partial = parse_rest_klines(&raw(&rows), &i, 1638747700000).unwrap();
    assert!(!partial[0].is_final);
    let final_rows = parse_rest_klines(&raw(&rows), &i, 1638747720000).unwrap();
    assert!(final_rows[0].is_final);
    assert!(parse_rest_klines(&raw(&json!([row, row])), &i, 1638747720000).is_err());
    assert!(parse_rest_klines(&raw(&rows), &i, 1638747659999).is_err());
    let mut bad = row.clone();
    bad[6] = json!(1638747959999_i64);
    assert!(parse_rest_klines(&raw(&json!([bad])), &i, 1638748000000).is_err());
}
#[test]
fn open_interest_rejects_wrong_symbol_venue_market_or_units() {
    let payload = br#"{"openInterest":"10659.509","symbol":"BTCUSDT","time":1589437530011}"#;
    let mut i = instrument(Market::UsdtPerpetual);
    assert_eq!(
        parse_open_interest(payload, &i).unwrap().value.to_string(),
        "10659.509"
    );
    i.symbol = "ETHUSDT".into();
    assert!(parse_open_interest(payload, &i).is_err());
    i = instrument(Market::Spot);
    assert!(parse_open_interest(payload, &i).is_err());
    i = instrument(Market::UsdtPerpetual);
    i.venue = Venue::Bybit;
    assert!(parse_open_interest(payload, &i).is_err());
}
#[test]
fn instrument_discovery_keeps_status_and_filters_dated_non_usdt_contracts() {
    let row = json!({"symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","status":"TRADING","contractType":"PERPETUAL","onboardDate":1569398400000_i64,"deliveryDate":4133404800000_i64});
    let mut inactive = row.clone();
    inactive["symbol"] = json!("ETHUSDT");
    inactive["baseAsset"] = json!("ETH");
    inactive["status"] = json!("PENDING_TRADING");
    let mut dated = row.clone();
    dated["symbol"] = json!("BTCUSDT_261225");
    dated["contractType"] = json!("CURRENT_QUARTER");
    let mut usdc = row.clone();
    usdc["symbol"] = json!("BTCUSDC");
    usdc["quoteAsset"] = json!("USDC");
    usdc["marginAsset"] = json!("USDC");
    let output = parse_instruments(
        &raw(&json!({"symbols":[row,inactive,dated,usdc]})),
        Market::UsdtPerpetual,
    )
    .unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(output[1].status, "PENDING_TRADING");
    assert_eq!(output[0].delivery_time_ms, Some(4133404800000));
    let spot = json!({"symbols":[{"symbol":"ETHBTC","baseAsset":"ETH","quoteAsset":"BTC","status":"TRADING"}]});
    assert_eq!(
        parse_instruments(&raw(&spot), Market::Spot).unwrap()[0].quote_asset,
        "BTC"
    );
    assert!(parse_instruments(&raw(&json!({"symbols":[row,row]})), Market::UsdtPerpetual).is_err());
}
#[test]
fn malformed_duplicate_keys_and_size_limits_fail_boundedly() {
    assert_eq!(
        parse_websocket(br#"{"e":"kline","e":"forceOrder"}"#, Market::Spot),
        Err(ParseError::MalformedJson)
    );
    assert!(matches!(
        parse_websocket(&vec![b' '; 8 * 1024 * 1024 + 1], Market::Spot),
        Err(ParseError::Limit(_))
    ));
    assert!(parse_websocket(br#"{"result":null,"id":1}"#, Market::Spot).is_err());
}

#[test]
fn all_market_funding_preserves_rows_with_zero_next_funding_sentinel() {
    let zero_time = json!({"e":"markPriceUpdate","E":1792085027002_i64,
        "s":"WAVESUSDT","p":"0.75728182","ap":"0.75728182","P":"0.00000000",
        "i":"0.75728182","r":"0.00000000","T":0,"st":1});
    let events = parse_websocket(
        &raw(&json!([mark("BTCUSDT"), zero_time])),
        Market::UsdtPerpetual,
    )
    .unwrap();
    assert_eq!(events.len(), 2);
    let MarketEvent::Funding(btc) = &events[0] else {
        unreachable!()
    };
    let MarketEvent::Funding(waves) = &events[1] else {
        unreachable!()
    };
    assert_eq!(btc.next_funding_time_ms, Some(1562306400000));
    assert_eq!(waves.next_funding_time_ms, None);
    assert_eq!(waves.rate, Decimal::ZERO);
    assert_eq!(waves.mark_price, Some(Decimal::new(75728182, 8)));
}
