//! Bounded public transport. Share each budget across clients using the same provider/IP.
//! Responses precede parsing; callers own archival, reconnects, backfill and acknowledgements.
use super::Market;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, Url, header::HeaderMap};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::{
    net::TcpStream,
    sync::Mutex,
    time::{Instant, sleep_until, timeout_at},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

const MAX_BYTES: usize = 4 * 1_048_576;

#[derive(Debug)]
pub enum NetworkError {
    InvalidConfiguration,
    Deadline,
    BudgetWait {
        retry_after: Duration,
    },
    Http {
        status: u16,
        retry_after: Option<Duration>,
    },
    Transport,
    ResponseTooLarge,
    Protocol,
    Closed,
}
impl std::fmt::Display for NetworkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "public feed transport: {self:?}")
    }
}
impl std::error::Error for NetworkError {}

struct Reservation {
    at: Instant,
    weight: u32,
    pending_id: Option<u64>,
}
struct BudgetState {
    capacity: u32,
    entries: VecDeque<Reservation>,
    next_id: u64,
    used: u32,
    blocked_until: Instant,
}
/// Rolling local reservations plus observed provider weight. Charges are never refunded.
/// The usable capacity is 80% of the supplied provider limit; other processes need coordination.
pub struct RequestBudget {
    window: Duration,
    state: Mutex<BudgetState>,
}
impl RequestBudget {
    pub fn new(limit: u32, window: Duration) -> Result<Arc<Self>, NetworkError> {
        if !(2..=1_000_000).contains(&limit)
            || window.is_zero()
            || window > Duration::from_secs(86400)
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        Ok(Arc::new(Self {
            window,
            state: Mutex::new(BudgetState {
                capacity: limit.saturating_mul(4).div_euclid(5),
                entries: VecDeque::new(),
                next_id: 0,
                used: 0,
                blocked_until: Instant::now(),
            }),
        }))
    }
    async fn reserve(&self, weight: u32, deadline: Instant) -> Result<u64, NetworkError> {
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(NetworkError::Deadline);
            }
            let mut state = self.state.lock().await;
            if weight == 0 || weight > state.capacity {
                return Err(NetworkError::InvalidConfiguration);
            }
            while state
                .entries
                .front()
                .is_some_and(|entry| entry.at + self.window <= now)
            {
                if let Some(entry) = state.entries.pop_front() {
                    state.used -= entry.weight;
                }
            }
            let mut wake = state.blocked_until;
            if state.used > state.capacity - weight {
                if let Some(entry) = state.entries.front() {
                    wake = wake.max(entry.at + self.window);
                }
            }
            if wake <= now {
                let id = state.next_id;
                state.next_id = state.next_id.wrapping_add(1);
                state.entries.push_back(Reservation {
                    at: now,
                    weight,
                    pending_id: Some(id),
                });
                state.used = state.used.saturating_add(weight);
                return Ok(id);
            }
            drop(state);
            if wake >= deadline {
                return Err(NetworkError::BudgetWait {
                    retry_after: wake.duration_since(now),
                });
            }
            sleep_until(wake).await;
        }
    }
    async fn observe(&self, headers: &HeaderMap, status: u16, completed_id: Option<u64>) {
        let now = Instant::now();
        let mut state = self.state.lock().await;
        while state
            .entries
            .front()
            .is_some_and(|entry| entry.at + self.window <= now)
        {
            if let Some(entry) = state.entries.pop_front() {
                state.used -= entry.weight;
            }
        }
        for entry in &mut state.entries {
            if entry.pending_id == completed_id {
                entry.pending_id = None;
            }
        }
        // Other in-flight requests may not yet be reflected in this response's provider count.
        let pending: u32 = state
            .entries
            .iter()
            .filter(|entry| entry.pending_id.is_some())
            .map(|entry| entry.weight)
            .sum();
        // Conservatively retain external usage for a whole rolling window after observation.
        let observed = header_number(headers, "x-mbx-used-weight-1m")
            .or_else(|| header_number(headers, "x-mbx-used-weight"));
        if let Some(observed) = observed.and_then(|n| u32::try_from(n).ok()) {
            let effective = observed.saturating_add(pending);
            if effective > state.used {
                let extra = effective - state.used;
                state.entries.push_back(Reservation {
                    at: now,
                    weight: extra,
                    pending_id: None,
                });
                state.used = effective;
            }
        }
        let mut delay = retry_after(headers);
        if status == 418 || status == 429 {
            delay = Some(
                delay
                    .unwrap_or(Duration::from_secs(if status == 418 { 120 } else { 60 }))
                    .max(Duration::from_secs(1)),
            );
        }
        if let (Some(limit), Some(remaining)) = (
            header_number(headers, "x-bapi-limit"),
            header_number(headers, "x-bapi-limit-status"),
        ) {
            if remaining <= limit.div_euclid(5) {
                let reset_ms = header_number(headers, "x-bapi-limit-reset-timestamp").unwrap_or(0);
                let now_ms = u64::try_from(Utc::now().timestamp_millis()).unwrap_or(0);
                let wait = Duration::from_millis(reset_ms.saturating_sub(now_ms))
                    .max(Duration::from_secs(1));
                delay = Some(delay.unwrap_or_default().max(wait));
            }
        }
        if let Some(delay) = delay {
            // Provider-controlled timestamps are bounded before Instant arithmetic.
            state.blocked_until = state
                .blocked_until
                .max(now + delay.min(Duration::from_secs(604800)));
        }
    }
}
fn header_number(headers: &HeaderMap, name: &str) -> Option<u64> {
    headers.get(name)?.to_str().ok()?.parse().ok()
}
fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get("retry-after")?.to_str().ok()?;
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let date = DateTime::parse_from_rfc2822(value).ok()?;
    Some(Duration::from_millis(
        u64::try_from((date.timestamp_millis() - Utc::now().timestamp_millis()).max(0)).ok()?,
    ))
}

#[derive(Debug)]
pub struct RawResponse {
    pub status: u16,
    pub raw_body: Vec<u8>,
    pub headers: HeaderMap,
    pub request_weight: u32,
    pub request_started_at_ms: i64,
    pub received_at_ms: i64,
}
#[derive(Clone)]
pub struct PublicClient {
    http: Client,
    rest_budget: Arc<RequestBudget>,
    connection_budget: Arc<RequestBudget>,
}
impl PublicClient {
    pub fn new(
        rest_budget: Arc<RequestBudget>,
        connection_budget: Arc<RequestBudget>,
    ) -> Result<Self, NetworkError> {
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
    /// Apply a discovered provider limit to every client sharing this REST budget.
    /// Retains all charged usage, outstanding requests and provider cooldowns.
    pub async fn set_rest_limit(&self, limit: u32) -> Result<(), NetworkError> {
        if !(2..=1_000_000).contains(&limit) {
            return Err(NetworkError::InvalidConfiguration);
        }
        self.rest_budget.state.lock().await.capacity = limit.saturating_mul(4).div_euclid(5);
        Ok(())
    }
    /// Exactly one GET, including non-success raw responses. The caller supplies public routes/weights.
    pub async fn get(
        &self,
        url: &str,
        weight: u32,
        timeout: Duration,
    ) -> Result<RawResponse, NetworkError> {
        let deadline = request_deadline(timeout)?;
        let url = public_url(url, false)?;
        let reservation = self.rest_budget.reserve(weight, deadline).await?;
        let request_started_at_ms = Utc::now().timestamp_millis();
        let mut response = timeout_at(deadline, self.http.get(url).send())
            .await
            .map_err(|_| NetworkError::Deadline)?
            .map_err(|_| NetworkError::Transport)?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        self.rest_budget
            .observe(&headers, status, Some(reservation))
            .await;
        if status == 418 {
            self.connection_budget.observe(&headers, status, None).await;
        }
        if response.content_length().is_some_and(|n| n > 4_194_304) {
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
        Ok(RawResponse {
            status,
            raw_body,
            headers,
            request_weight: weight,
            request_started_at_ms,
            received_at_ms: Utc::now().timestamp_millis(),
        })
    }
    /// Empty subscriptions use URL-selected Binance streams. Nonempty subscriptions are Bybit topics.
    /// JSON acknowledgements and pongs are returned raw, so failures remain visible to the recorder.
    pub async fn connect(
        &self,
        url: &str,
        subscriptions: &[String],
        options: StreamOptions,
    ) -> Result<PublicStream, NetworkError> {
        options.validate()?;
        let url = public_url(url, true)?;
        if subscriptions.len() > 1500
            || subscriptions.iter().any(|s| s.len() > 100 || s.is_empty())
            || serde_json::to_vec(subscriptions)
                .map_err(|_| NetworkError::InvalidConfiguration)?
                .len()
                > 21000
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        let deadline = Instant::now() + options.lifetime;
        let connect_deadline = deadline.min(Instant::now() + Duration::from_secs(5));
        let reservation = self.connection_budget.reserve(1, connect_deadline).await?;
        let config = WebSocketConfig::default()
            .max_message_size(Some(options.max_message_bytes))
            .max_frame_size(Some(options.max_message_bytes));
        let (socket, _) = match timeout_at(
            connect_deadline,
            connect_async_with_config(url.as_str(), Some(config), false),
        )
        .await
        .map_err(|_| NetworkError::Deadline)?
        {
            Ok(connection) => connection,
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                let status = response.status().as_u16();
                self.connection_budget
                    .observe(response.headers(), status, Some(reservation))
                    .await;
                if status == 418 {
                    self.rest_budget
                        .observe(response.headers(), status, None)
                        .await;
                }
                return Err(NetworkError::Http {
                    status,
                    retry_after: retry_after(response.headers()),
                });
            }
            Err(_) => return Err(NetworkError::Transport),
        };
        let mut stream = PublicStream {
            socket,
            deadline,
            terminal: false,
            ping_interval: options.application_ping_interval,
            next_ping: options
                .application_ping_interval
                .map(|d| Instant::now() + d),
        };
        for (index, topics) in subscriptions.chunks(10).enumerate() {
            if index > 0 {
                timeout_at(deadline, tokio::time::sleep(Duration::from_millis(200)))
                    .await
                    .map_err(|_| NetworkError::Deadline)?;
            }
            stream.ping_if_due(deadline).await?;
            let payload =
                serde_json::json!({"op":"subscribe", "args": topics, "req_id": index.to_string()})
                    .to_string();
            stream.send(Message::Text(payload.into()), deadline).await?;
        }
        Ok(stream)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct StreamOptions {
    pub lifetime: Duration,
    pub max_message_bytes: usize,
    /// Set to 20 seconds for Bybit. None for Binance, which sends protocol pings.
    pub application_ping_interval: Option<Duration>,
}
impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            lifetime: Duration::from_secs(82_800),
            max_message_bytes: MAX_BYTES,
            application_ping_interval: None,
        }
    }
}
impl StreamOptions {
    fn validate(&self) -> Result<(), NetworkError> {
        if self.lifetime.is_zero()
            || self.lifetime >= Duration::from_secs(86400)
            || self.max_message_bytes == 0
            || self.max_message_bytes > MAX_BYTES
            || self
                .application_ping_interval
                .is_some_and(|d| d.is_zero() || d > Duration::from_secs(20))
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        Ok(())
    }
}
#[derive(Debug)]
pub struct RawFrame {
    pub raw_body: Vec<u8>,
    pub received_at_ms: i64,
}
pub struct PublicStream {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    deadline: Instant,
    terminal: bool,
    ping_interval: Option<Duration>,
    next_ping: Option<Instant>,
}
impl PublicStream {
    async fn send(&mut self, message: Message, deadline: Instant) -> Result<(), NetworkError> {
        timeout_at(
            deadline.min(Instant::now() + Duration::from_secs(5)),
            self.socket.send(message),
        )
        .await
        .map_err(|_| NetworkError::Deadline)?
        .map_err(|_| NetworkError::Transport)
    }
    async fn ping_if_due(&mut self, deadline: Instant) -> Result<(), NetworkError> {
        if self.next_ping.is_some_and(|at| at <= Instant::now()) {
            self.send(Message::Text(r#"{"op":"ping"}"#.into()), deadline)
                .await?;
            self.next_ping = self.ping_interval.map(|d| Instant::now() + d);
        }
        Ok(())
    }
    /// Cancellation or any error terminates the session. Call next continuously to service heartbeats.
    pub async fn next(&mut self, idle_timeout: Duration) -> Result<RawFrame, NetworkError> {
        if self.terminal {
            return Err(NetworkError::Closed);
        }
        self.terminal = true;
        let deadline = self.deadline.min(request_deadline(idle_timeout)?);
        loop {
            if Instant::now() >= deadline {
                return Err(NetworkError::Deadline);
            }
            self.ping_if_due(deadline).await?;
            let wake = self.next_ping.map_or(deadline, |at| at.min(deadline));
            let message = match timeout_at(wake, self.socket.next()).await {
                Err(_) if wake < deadline => continue,
                Err(_) => return Err(NetworkError::Deadline),
                Ok(None) => return Err(NetworkError::Closed),
                Ok(Some(Err(tokio_tungstenite::tungstenite::Error::Capacity(_)))) => {
                    return Err(NetworkError::ResponseTooLarge);
                }
                Ok(Some(Err(_))) => return Err(NetworkError::Transport),
                Ok(Some(Ok(message))) => message,
            };
            match message {
                Message::Text(text) => {
                    self.terminal = false;
                    return Ok(RawFrame {
                        raw_body: text.as_bytes().to_vec(),
                        received_at_ms: Utc::now().timestamp_millis(),
                    });
                }
                Message::Ping(payload) => self.send(Message::Pong(payload), deadline).await?,
                Message::Pong(_) => {}
                Message::Close(_) => {
                    let _ = timeout_at(deadline, self.socket.flush()).await;
                    return Err(NetworkError::Closed);
                }
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
fn public_url(url: &str, websocket: bool) -> Result<Url, NetworkError> {
    let url = Url::parse(url).map_err(|_| NetworkError::InvalidConfiguration)?;
    let tls = if websocket { "wss" } else { "https" };
    let plain = if websocket { "ws" } else { "http" };
    let loopback = url
        .host_str()
        .is_some_and(|host| matches!(host, "127.0.0.1" | "[::1]" | "localhost"));
    if (url.scheme() != tls && !(url.scheme() == plain && loopback))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(NetworkError::InvalidConfiguration);
    }
    Ok(url)
}
fn validate_symbols(symbols: &[String]) -> Result<(), NetworkError> {
    if symbols.is_empty()
        || symbols.len() > 500
        || symbols.iter().any(|s| {
            s.is_empty()
                || s.len() > 40
                || !s
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        })
    {
        return Err(NetworkError::InvalidConfiguration);
    }
    Ok(())
}
/// USD-M market streams use the /market lane (provider API reviewed 2026-10-15).
pub fn binance_combined_url(
    market: Market,
    symbols: &[String],
    mark_price: bool,
    liquidations: bool,
) -> Result<String, NetworkError> {
    validate_symbols(symbols)?;
    if market == Market::Spot && (mark_price || liquidations) {
        return Err(NetworkError::InvalidConfiguration);
    }
    let mut topics: Vec<String> = symbols
        .iter()
        .map(|s| format!("{}@kline_1m", s.to_ascii_lowercase()))
        .collect();
    if mark_price {
        topics.push("!markPrice@arr@1s".into());
    }
    if liquidations {
        topics.push("!forceOrder@arr".into());
    }
    let base = match market {
        Market::Spot => "wss://stream.binance.com:9443/stream",
        Market::UsdtPerpetual => "wss://fstream.binance.com/market/stream",
    };
    Ok(format!("{base}?streams={}", topics.join("/")))
}
pub fn bybit_topics(symbols: &[String], market: Market) -> Result<Vec<String>, NetworkError> {
    validate_symbols(symbols)?;
    Ok(symbols
        .iter()
        .flat_map(|symbol| {
            let mut topics = vec![format!("kline.1.{symbol}"), format!("publicTrade.{symbol}")];
            if market == Market::UsdtPerpetual {
                topics.push(format!("tickers.{symbol}"));
            }
            topics
        })
        .collect())
}
