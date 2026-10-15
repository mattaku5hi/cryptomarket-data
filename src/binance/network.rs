//! Optional bounded public transport. No retries, reconnects, signing or storage.
//! Share budgets between clients in one process; other processes/IP usage remain external.

use super::{
    DepthSnapshot, Instrument, MarketKind, ParseError, ParseLimits, TimestampUnit, WsEvent,
    parse_rest_depth_snapshot, parse_websocket,
};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, Url, header::HeaderMap};
use std::{
    collections::VecDeque,
    error::Error,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::Mutex,
    time::{Instant, sleep_until, timeout_at},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

const MAX_BYTES: usize = 1_048_576;
const MAX_BYTES_U64: u64 = 1_048_576;

#[derive(Debug)]
pub enum NetworkError {
    InvalidConfiguration,
    BudgetStopped,
    Deadline,
    Transport,
    Http {
        status: u16,
        retry_after_seconds: Option<u64>,
    },
    ResponseTooLarge,
    Protocol,
    Closed,
    FrameLimit,
    Parse(ParseError),
}
impl fmt::Display for NetworkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "public Binance transport: {self:?}")
    }
}
impl Error for NetworkError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Parse(error) => Some(error),
            _ => None,
        }
    }
}

struct BudgetState {
    entries: VecDeque<(Instant, u32)>,
    used: u32,
}
/// Proactive rolling-window budget; a reservation is charged before network I/O.
/// Cancellation/failure does not refund it. This is a local limit, not exchangeInfo qualification.
pub struct RequestBudget {
    limit: u32,
    window: Duration,
    stopped: AtomicBool,
    state: Mutex<BudgetState>,
}
impl RequestBudget {
    pub fn new(limit: u32, window: Duration) -> Result<Arc<Self>, NetworkError> {
        if limit == 0 || limit > 10_000 || window.is_zero() || window > Duration::from_secs(86_400)
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        Ok(Arc::new(Self {
            limit,
            window,
            stopped: AtomicBool::new(false),
            state: Mutex::new(BudgetState {
                entries: VecDeque::new(),
                used: 0,
            }),
        }))
    }
    /// Latch all clones after throttling/ban or an external coordinator's refusal.
    /// No automatic reset: a caller must resolve the cause before creating a replacement budget.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }

    async fn reserve(&self, cost: u32, deadline: Instant) -> Result<(), NetworkError> {
        if cost == 0 || cost > self.limit {
            return Err(NetworkError::InvalidConfiguration);
        }
        loop {
            let mut state = self.state.lock().await;
            let now = Instant::now();
            if now >= deadline {
                return Err(NetworkError::Deadline);
            }
            if self.stopped.load(Ordering::Acquire) {
                return Err(NetworkError::BudgetStopped);
            }
            while state
                .entries
                .front()
                .is_some_and(|(at, _)| now.duration_since(*at) >= self.window)
            {
                if let Some((_, weight)) = state.entries.pop_front() {
                    state.used -= weight;
                }
            }
            if state.used <= self.limit - cost {
                state.entries.push_back((now, cost));
                state.used += cost;
                return Ok(());
            }
            let wake = state
                .entries
                .front()
                .map(|(at, _)| *at + self.window)
                .ok_or(NetworkError::InvalidConfiguration)?;
            drop(state);
            if wake >= deadline {
                return Err(NetworkError::Deadline);
            }
            sleep_until(wake).await;
        }
    }
}

/// An SDK read with complete raw body, headers and local availability evidence.
/// Raw HTTP failure/partial-body archival remains the recorder's responsibility.
pub struct DepthResponse {
    pub snapshot: DepthSnapshot,
    pub raw_body: Vec<u8>,
    pub headers: HeaderMap,
    pub request_weight: u32,
    pub request_started_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct PublicClient {
    http: Client,
    rest_budget: Arc<RequestBudget>,
    connection_budget: Arc<RequestBudget>,
}
impl PublicClient {
    /// Supply separately shared REST-weight and WS-connect budgets. No provider limit is guessed.
    pub fn new(
        rest_budget: Arc<RequestBudget>,
        connection_budget: Arc<RequestBudget>,
    ) -> Result<Self, NetworkError> {
        if rest_budget.window < Duration::from_secs(60)
            || connection_budget.window < Duration::from_secs(300)
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        let http = Client::builder()
            .user_agent("market-data/0.1 public-reader")
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|_| NetworkError::InvalidConfiguration)?;
        Ok(Self {
            http,
            rest_budget,
            connection_budget,
        })
    }

    pub async fn depth(
        &self,
        instrument: Instrument,
        limit: usize,
        timeout: Duration,
    ) -> Result<DepthResponse, NetworkError> {
        let deadline = request_deadline(timeout)?;
        let weight = depth_weight(instrument.market, limit)?;
        let base = match instrument.market {
            MarketKind::Spot => "https://api.binance.com/api/v3/depth",
            MarketKind::UsdMPerpetual => "https://fapi.binance.com/fapi/v1/depth",
        };
        let mut url = Url::parse(base).map_err(|_| NetworkError::InvalidConfiguration)?;
        url.query_pairs_mut()
            .append_pair("symbol", instrument.symbol.as_str())
            .append_pair("limit", &limit.to_string());
        self.rest_budget.reserve(weight, deadline).await?;
        let request_started_at = Utc::now();
        let mut response = timeout_at(deadline, self.http.get(url).send())
            .await
            .map_err(|_| NetworkError::Deadline)?
            .map_err(|_| NetworkError::Transport)?;
        let headers = response.headers().clone();
        let status = response.status().as_u16();
        if status == 429 || status == 418 {
            self.rest_budget.stop();
        }
        if status == 418 {
            self.connection_budget.stop();
        }
        if !response.status().is_success() {
            return Err(NetworkError::Http {
                status,
                retry_after_seconds: headers
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse().ok()),
            });
        }
        if response.content_length().is_some_and(|n| n > MAX_BYTES_U64) {
            return Err(NetworkError::ResponseTooLarge);
        }
        let mut raw_body = Vec::new();
        while let Some(chunk) = timeout_at(deadline, response.chunk())
            .await
            .map_err(|_| NetworkError::Deadline)?
            .map_err(|_| NetworkError::Transport)?
        {
            if chunk.len() > MAX_BYTES.saturating_sub(raw_body.len()) {
                return Err(NetworkError::ResponseTooLarge);
            }
            raw_body.extend_from_slice(&chunk);
        }
        let received_at = Utc::now();
        let snapshot = parse_rest_depth_snapshot(
            &raw_body,
            instrument,
            ParseLimits {
                max_bytes: MAX_BYTES,
                max_levels_per_side: limit,
            },
        )
        .map_err(NetworkError::Parse)?;
        if Instant::now() >= deadline {
            return Err(NetworkError::Deadline);
        }
        Ok(DepthResponse {
            snapshot,
            raw_body,
            headers,
            request_weight: weight,
            request_started_at,
            received_at,
            available_at: Utc::now(),
        })
    }

    pub async fn connect(
        &self,
        instrument: Instrument,
        feed: Feed,
        lifetime: Duration,
        max_frames: u64,
    ) -> Result<PublicStream, NetworkError> {
        if lifetime.is_zero()
            || lifetime > Duration::from_secs(82_800)
            || max_frames == 0
            || max_frames > 100_000
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        let deadline = Instant::now() + lifetime;
        let connect_deadline = deadline.min(Instant::now() + Duration::from_secs(5));
        self.connection_budget.reserve(1, connect_deadline).await?;
        let endpoint = stream_url(&instrument, feed)?;
        let mut config = WebSocketConfig::default();
        config.max_message_size = Some(MAX_BYTES);
        config.max_frame_size = Some(MAX_BYTES);
        let connection = timeout_at(
            connect_deadline,
            connect_async_with_config(endpoint.as_str(), Some(config), false),
        )
        .await
        .map_err(|_| NetworkError::Deadline)?;
        let (socket, _) = match connection {
            Ok(connection) => connection,
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                let status = response.status().as_u16();
                if status == 429 || status == 418 {
                    self.connection_budget.stop();
                }
                if status == 418 {
                    self.rest_budget.stop();
                }
                return Err(NetworkError::Http {
                    status,
                    retry_after_seconds: response
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse().ok()),
                });
            }
            Err(_) => return Err(NetworkError::Transport),
        };
        Ok(PublicStream {
            socket,
            instrument,
            feed,
            deadline,
            remaining_frames: max_frames,
            terminal: false,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Feed {
    AggTrade,
    Depth100ms,
}

pub struct PublicObservation {
    pub event: WsEvent,
    pub raw_body: Vec<u8>,
    pub received_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
}
pub struct PublicStream {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    instrument: Instrument,
    feed: Feed,
    deadline: Instant,
    remaining_frames: u64,
    terminal: bool,
}
impl PublicStream {
    /// Any error/cancellation latches this session; drop/close it and explicitly resynchronize.
    /// No reconnect, missing-trade inference or candle finality is manufactured.
    pub async fn next(
        &mut self,
        idle_timeout: Duration,
    ) -> Result<PublicObservation, NetworkError> {
        if self.terminal {
            return Err(NetworkError::Closed);
        }
        self.terminal = true;
        let deadline = self.deadline.min(request_deadline(idle_timeout)?);
        loop {
            if self.remaining_frames == 0 {
                return Err(NetworkError::FrameLimit);
            }
            let message = timeout_at(deadline, self.socket.next())
                .await
                .map_err(|_| NetworkError::Deadline)?
                .ok_or(NetworkError::Closed)?
                .map_err(|_| NetworkError::Transport)?;
            self.remaining_frames -= 1;
            let received_at = Utc::now();
            match message {
                Message::Text(text) => {
                    let raw_body = text.as_bytes().to_vec();
                    let event = parse_websocket(
                        &raw_body,
                        self.instrument.clone(),
                        TimestampUnit::Milliseconds,
                        ParseLimits::default(),
                    )
                    .map_err(NetworkError::Parse)?;
                    if !matches!(
                        (&event, self.feed),
                        (WsEvent::AggTrade(_), Feed::AggTrade)
                            | (WsEvent::DepthUpdate(_), Feed::Depth100ms)
                    ) {
                        return Err(NetworkError::Protocol);
                    }
                    if Instant::now() >= deadline {
                        return Err(NetworkError::Deadline);
                    }
                    self.terminal = false;
                    return Ok(PublicObservation {
                        event,
                        raw_body,
                        received_at,
                        available_at: Utc::now(),
                    });
                }
                Message::Ping(payload) => {
                    timeout_at(deadline, self.socket.send(Message::Pong(payload)))
                        .await
                        .map_err(|_| NetworkError::Deadline)?
                        .map_err(|_| NetworkError::Transport)?
                }
                Message::Pong(_) => {}
                Message::Close(_) => return Err(NetworkError::Closed),
                _ => return Err(NetworkError::Protocol),
            }
        }
    }
    pub async fn close(&mut self) -> Result<(), NetworkError> {
        self.terminal = true;
        timeout_at(
            Instant::now() + Duration::from_secs(2),
            self.socket.close(None),
        )
        .await
        .map_err(|_| NetworkError::Deadline)?
        .map_err(|_| NetworkError::Transport)
    }
}

fn request_deadline(timeout: Duration) -> Result<Instant, NetworkError> {
    if timeout.is_zero() || timeout > Duration::from_secs(30) {
        return Err(NetworkError::InvalidConfiguration);
    }
    Ok(Instant::now() + timeout)
}

fn depth_weight(market: MarketKind, limit: usize) -> Result<u32, NetworkError> {
    match (market, limit) {
        (MarketKind::Spot, 1..=100) => Ok(5),
        (MarketKind::Spot, 101..=500) => Ok(25),
        (MarketKind::Spot, 501..=1000) => Ok(50),
        (MarketKind::Spot, 1001..=5000) => Ok(250),
        (MarketKind::UsdMPerpetual, 5 | 10 | 20 | 50) => Ok(2),
        (MarketKind::UsdMPerpetual, 100) => Ok(5),
        (MarketKind::UsdMPerpetual, 500) => Ok(10),
        (MarketKind::UsdMPerpetual, 1000) => Ok(20),
        _ => Err(NetworkError::InvalidConfiguration),
    }
}

fn stream_url(instrument: &Instrument, feed: Feed) -> Result<Url, NetworkError> {
    let (base, prefix) = match (instrument.market, feed) {
        (MarketKind::Spot, _) => ("wss://stream.binance.com:9443", "/ws/"),
        (MarketKind::UsdMPerpetual, Feed::AggTrade) => ("wss://fstream.binance.com", "/market/ws/"),
        (MarketKind::UsdMPerpetual, Feed::Depth100ms) => {
            ("wss://fstream.binance.com", "/public/ws/")
        }
    };
    let suffix = match feed {
        Feed::AggTrade => "@aggTrade",
        Feed::Depth100ms => "@depth@100ms",
    };
    let mut url = Url::parse(base).map_err(|_| NetworkError::InvalidConfiguration)?;
    url.set_path(&format!(
        "{prefix}{}{suffix}",
        instrument.symbol.as_str().to_lowercase()
    ));
    Ok(url)
}
