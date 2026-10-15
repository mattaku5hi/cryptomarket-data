//! Finite public trade-candle example; no REST polling or automatic reconnect.
use market_data::{
    binance::{
        Instrument, MarketKind, Symbol, WsEvent,
        network::{Feed, NetworkError, PublicClient, RequestBudget},
    },
    candles::{CandleCadence, TradeCandleBuilder, TradeObservation, VolumeConvention},
    types::{ClockStatus, InstrumentId},
};
use serde_json::json;
use std::{error::Error, time::Duration};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [product, symbol, seconds, frames] = args.as_slice() else {
        return Err("usage: candles spot|usdm SYMBOL FIXED_SECONDS MAX_FRAMES".into());
    };
    let market = match product.as_str() {
        "spot" => MarketKind::Spot,
        "usdm" => MarketKind::UsdMPerpetual,
        _ => return Err("product must be spot or usdm".into()),
    };
    let instrument = Instrument {
        market,
        symbol: Symbol::new(symbol.clone())?,
    };
    let source = InstrumentId::new(format!("binance:{product}:{symbol}"))
        .ok_or("invalid instrument identity")?;
    let cadence = CandleCadence::from_seconds(seconds.parse()?)?;
    let frames: u64 = frames.parse()?;
    if frames == 0 || frames > 100_000 {
        return Err("frame bound must be 1..100000".into());
    }
    let mut builder =
        TradeCandleBuilder::new(source.clone(), cadence, VolumeConvention::TotalBase)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let client = PublicClient::new(
            RequestBudget::new(120, Duration::from_secs(60))?,
            RequestBudget::new(30, Duration::from_secs(300))?,
        )?;
        let mut stream = client.connect(instrument, Feed::AggTrade, Duration::from_secs(300), frames).await?;
        for _ in 0..frames {
            let observation = match stream.next(Duration::from_secs(10)).await {
                Ok(observation) => observation,
                Err(NetworkError::FrameLimit) => break,
                Err(error) => return Err(error.into()),
            };
            let WsEvent::AggTrade(trade) = observation.event else {
                return Err::<(), Box<dyn Error>>("unexpected provider event".into());
            };
            let timestamp = i64::try_from(trade.trade_time.value)?;
            let trade_time = chrono::DateTime::from_timestamp_millis(timestamp)
                .ok_or("trade timestamp outside supported range")?;
            let candle = builder.push(&TradeObservation {
                source: source.clone(), trade_time, aggregate_trade_id: trade.aggregate_trade_id,
                available_at: observation.available_at, clock_status: ClockStatus::Unknown,
                price: trade.price.as_string().parse()?, base_volume: trade.quantity.as_string().parse()?,
            })?;
            if let Some(candle) = candle {
                println!("{}", json!({
                    "instrument": candle.source.as_str(), "seconds": candle.cadence.width_seconds(),
                    "start": candle.window_start, "end": candle.window_end,
                    "open": candle.open, "high": candle.high, "low": candle.low, "close": candle.close,
                    "base_volume": candle.base_volume, "vwap": candle.observed_trade_vwap,
                    "available_at": candle.available_at, "skipped_intervals": candle.skipped_intervals,
                    "clock_status": "unknown", "source_health": "unqualified",
                }));
            }
        }
        stream.close().await?;
        Ok(())
    })
}
