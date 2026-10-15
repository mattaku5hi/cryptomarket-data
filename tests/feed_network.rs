#![cfg(feature = "network")]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use futures_util::{SinkExt, StreamExt};
use market_data::feeds::{
    Market,
    network::{
        NetworkError, PublicClient, RequestBudget, StreamOptions, binance_combined_url,
        bybit_topics,
    },
};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::Arc,
    time::Duration,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

fn run(future: impl std::future::Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future);
}
fn client() -> PublicClient {
    let rest = RequestBudget::new(100, Duration::from_secs(60)).unwrap();
    let connections = RequestBudget::new(100, Duration::from_secs(300)).unwrap();
    PublicClient::new(rest, connections).unwrap()
}
fn http_response(response: String) -> (String, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/public", listener.local_addr().unwrap());
    let thread = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut bytes = [0u8; 4096];
        let size = socket.read(&mut bytes).unwrap();
        assert!(String::from_utf8_lossy(bytes.get(..size).unwrap()).starts_with("GET /public "));
        socket.write_all(response.as_bytes()).unwrap();
    });
    (url, thread)
}
#[test]
fn rest_preserves_raw_and_learns_shared_weight() {
    run(async {
        let (url, server) = http_response("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nX-MBX-USED-WEIGHT-1M: 80\r\nConnection: close\r\n\r\n{}".into());
        let rest = RequestBudget::new(100, Duration::from_secs(60)).unwrap();
        let connections = RequestBudget::new(100, Duration::from_secs(300)).unwrap();
        let first = PublicClient::new(Arc::clone(&rest), Arc::clone(&connections)).unwrap();
        let second = PublicClient::new(rest, connections).unwrap();
        let response = first.get(&url, 1, Duration::from_secs(2)).await.unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.raw_body, b"{}");
        assert_eq!(response.headers.get("x-mbx-used-weight-1m").unwrap(), "80");
        assert!(response.received_at_ms >= response.request_started_at_ms);
        assert!(matches!(
            second.get(&url, 1, Duration::from_millis(20)).await,
            Err(NetworkError::BudgetWait { .. })
        ));
        server.join().unwrap();
    });
}
#[test]
fn local_reservations_stop_at_eighty_percent_without_headers() {
    run(async {
        let (url, server) = http_response(
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
        );
        let client = client();
        client.get(&url, 80, Duration::from_secs(2)).await.unwrap();
        assert!(matches!(
            client.get(&url, 1, Duration::from_millis(20)).await,
            Err(NetworkError::BudgetWait { .. })
        ));
        server.join().unwrap();
    });
}
#[test]
fn throttle_returns_body_and_blocks_clones_and_ban_blocks_connections() {
    run(async {
        for status in [429, 418] {
            let (url, server) = http_response(format!(
                "HTTP/1.1 {status} Slow\r\nContent-Length: 4\r\nRetry-After: 2\r\nConnection: close\r\n\r\nslow"
            ));
            let client = client();
            let response = client.get(&url, 1, Duration::from_secs(2)).await.unwrap();
            assert_eq!(response.status, status);
            assert_eq!(response.raw_body, b"slow");
            match client.clone().get(&url, 1, Duration::from_millis(20)).await {
                Err(NetworkError::BudgetWait { retry_after }) => {
                    assert!(retry_after > Duration::from_secs(1))
                }
                other => assert!(
                    matches!(other, Err(NetworkError::BudgetWait { .. })),
                    "expected cooldown: {other:?}"
                ),
            }
            if status == 418 {
                let options = StreamOptions {
                    lifetime: Duration::from_millis(20),
                    ..Default::default()
                };
                assert!(matches!(
                    client
                        .connect(&url.replace("http:", "ws:"), &[], options)
                        .await,
                    Err(NetworkError::BudgetWait { .. })
                ));
            }
            server.join().unwrap();
        }
    });
}
#[test]
fn bybit_rate_header_and_rest_size_are_bounded() {
    run(async {
        let (url, server) = http_response("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nX-Bapi-Limit: 10\r\nX-Bapi-Limit-Status: 2\r\nConnection: close\r\n\r\n".into());
        let client = client();
        client.get(&url, 1, Duration::from_secs(2)).await.unwrap();
        assert!(matches!(
            client.get(&url, 1, Duration::from_millis(20)).await,
            Err(NetworkError::BudgetWait { .. })
        ));
        server.join().unwrap();
        let (url, server) = http_response(
            "HTTP/1.1 200 OK\r\nContent-Length: 4194305\r\nConnection: close\r\n\r\n".into(),
        );
        // A fresh client has no cooldown from the preceding endpoint.
        let fresh = PublicClient::new(
            RequestBudget::new(100, Duration::from_secs(60)).unwrap(),
            RequestBudget::new(100, Duration::from_secs(300)).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            fresh.get(&url, 1, Duration::from_secs(2)).await,
            Err(NetworkError::ResponseTooLarge)
        ));
        server.join().unwrap();
    });
}
#[test]
fn expired_window_allows_the_next_request() {
    run(async {
        let budget = RequestBudget::new(100, Duration::from_millis(20)).unwrap();
        let client = PublicClient::new(
            budget,
            RequestBudget::new(100, Duration::from_secs(300)).unwrap(),
        )
        .unwrap();
        let (url, first) = http_response(
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
        );
        client.get(&url, 80, Duration::from_secs(2)).await.unwrap();
        first.join().unwrap();
        let (url, second) = http_response(
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
        );
        client.get(&url, 80, Duration::from_secs(2)).await.unwrap();
        second.join().unwrap();
    });
}
#[test]
fn websocket_subscribes_preserves_ack_services_both_pings_and_close() {
    run(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(socket).await.unwrap();
            let subscription = socket.next().await.unwrap().unwrap();
            let json: serde_json::Value =
                serde_json::from_str(subscription.to_text().unwrap()).unwrap();
            assert_eq!(json.get("op").unwrap(), "subscribe");
            assert_eq!(
                json.get("args").unwrap(),
                &serde_json::json!(["kline.1.BTCUSDT"])
            );
            socket
                .send(Message::Text(r#"{"success":true,"op":"subscribe"}"#.into()))
                .await
                .unwrap();
            socket
                .send(Message::Ping(b"check".to_vec().into()))
                .await
                .unwrap();
            let pong = socket.next().await.unwrap().unwrap();
            assert_eq!(pong, Message::Pong(b"check".to_vec().into()));
            let ping = socket.next().await.unwrap().unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(ping.to_text().unwrap())
                    .unwrap()
                    .get("op")
                    .unwrap(),
                "ping"
            );
            socket
                .send(Message::Text(r#"{"op":"pong"}"#.into()))
                .await
                .unwrap();
            socket.close(None).await.unwrap();
        });
        let options = StreamOptions {
            application_ping_interval: Some(Duration::from_millis(50)),
            lifetime: Duration::from_secs(3),
            ..Default::default()
        };
        let mut stream = client()
            .connect(&url, &["kline.1.BTCUSDT".into()], options)
            .await
            .unwrap();
        assert_eq!(
            stream.next(Duration::from_secs(2)).await.unwrap().raw_body,
            br#"{"success":true,"op":"subscribe"}"#
        );
        assert_eq!(
            stream.next(Duration::from_secs(2)).await.unwrap().raw_body,
            br#"{"op":"pong"}"#
        );
        assert!(matches!(
            stream.next(Duration::from_secs(2)).await,
            Err(NetworkError::Closed)
        ));
        assert!(matches!(
            stream.next(Duration::from_secs(2)).await,
            Err(NetworkError::Closed)
        ));
        server.await.unwrap();
    });
}
#[test]
fn websocket_oversize_and_idle_timeout_terminate_session() {
    run(async {
        for oversized in [true, false] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("ws://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(socket).await.unwrap();
                if oversized {
                    socket
                        .send(Message::Text("x".repeat(100).into()))
                        .await
                        .unwrap();
                }
                let _ = socket.next().await;
            });
            let options = StreamOptions {
                max_message_bytes: 32,
                ..Default::default()
            };
            let mut stream = client().connect(&url, &[], options).await.unwrap();
            let result = stream
                .next(if oversized {
                    Duration::from_secs(2)
                } else {
                    Duration::from_millis(30)
                })
                .await;
            if oversized {
                assert!(matches!(result, Err(NetworkError::ResponseTooLarge)));
            } else {
                assert!(matches!(result, Err(NetworkError::Deadline)));
            }
            assert!(matches!(
                stream.next(Duration::from_secs(1)).await,
                Err(NetworkError::Closed)
            ));
            drop(stream);
            server.await.unwrap();
        }
    });
}
#[test]
fn builders_cover_five_hundred_symbols_and_current_market_lane() {
    let symbols: Vec<String> = (0..500).map(|n| format!("COIN{n}USDT")).collect();
    let url = binance_combined_url(Market::UsdtPerpetual, &symbols, true, true).unwrap();
    assert!(url.starts_with("wss://fstream.binance.com/market/stream?streams="));
    assert!(url.contains("coin499usdt@kline_1m"));
    assert!(url.ends_with("/!markPrice@arr@1s/!forceOrder@arr"));
    assert_eq!(
        bybit_topics(&symbols, Market::UsdtPerpetual).unwrap().len(),
        1500
    );
    assert_eq!(bybit_topics(&symbols, Market::Spot).unwrap().len(), 1000);
    assert!(binance_combined_url(Market::Spot, &symbols, true, false).is_err());
    assert!(bybit_topics(&["BTCUSDT/evil".into()], Market::Spot).is_err());
}

#[test]
fn provider_usage_keeps_inflight_reservations_charged() {
    run(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/public", listener.local_addr().unwrap());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            first
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut bytes = [0u8; 4096];
            assert!(first.read(&mut bytes).unwrap() > 0);
            started_tx.send(()).unwrap();
            let (mut second, _) = listener.accept().unwrap();
            second
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            assert!(second.read(&mut bytes).unwrap() > 0);
            second.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nX-MBX-USED-WEIGHT-1M: 70\r\nConnection: close\r\n\r\n").unwrap();
            release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            first
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .unwrap();
        });
        let client = client();
        let first_client = client.clone();
        let first_url = url.clone();
        let first = tokio::spawn(async move {
            first_client
                .get(&first_url, 20, Duration::from_secs(3))
                .await
        });
        started_rx.await.unwrap();
        client.get(&url, 1, Duration::from_secs(2)).await.unwrap();
        // Header usage 70 plus still-pending weight 20 already exceeds the usable 80.
        let blocked = client.get(&url, 1, Duration::from_millis(20)).await;
        release_tx.send(()).unwrap();
        assert!(matches!(blocked, Err(NetworkError::BudgetWait { .. })));
        first.await.unwrap().unwrap();
        server.join().unwrap();
    });
}

#[test]
fn discovered_limit_keeps_shared_usage_and_cooldowns() {
    run(async {
        let (url, server) = http_response(
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
        );
        let client = client();
        let shared = client.clone();
        client.get(&url, 20, Duration::from_secs(2)).await.unwrap();
        client.set_rest_limit(25).await.unwrap();
        assert!(matches!(
            shared.get(&url, 1, Duration::from_millis(20)).await,
            Err(NetworkError::BudgetWait { .. })
        ));
        assert!(matches!(
            client.set_rest_limit(0).await,
            Err(NetworkError::InvalidConfiguration)
        ));
        server.join().unwrap();

        let (url, server) = http_response(
            "HTTP/1.1 429 Slow\r\nContent-Length: 0\r\nRetry-After: 2\r\nConnection: close\r\n\r\n"
                .into(),
        );
        client.set_rest_limit(100).await.unwrap();
        shared.get(&url, 1, Duration::from_secs(2)).await.unwrap();
        client.set_rest_limit(1000).await.unwrap();
        assert!(
            matches!(shared.get(&url, 1, Duration::from_millis(20)).await,
            Err(NetworkError::BudgetWait { retry_after }) if retry_after > Duration::from_secs(1))
        );
        server.join().unwrap();
    });
}

#[test]
fn forbidden_response_stops_both_transports_for_ten_minutes() {
    run(async {
        let (url, server) = http_response(
            "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
        );
        let client = client();
        assert_eq!(
            client
                .get(&url, 1, Duration::from_secs(2))
                .await
                .unwrap()
                .status,
            403
        );
        assert!(
            matches!(client.get(&url, 1, Duration::from_millis(20)).await,
            Err(NetworkError::BudgetWait { retry_after }) if retry_after > Duration::from_secs(599))
        );
        let options = StreamOptions {
            lifetime: Duration::from_millis(20),
            ..Default::default()
        };
        assert!(
            matches!(client.connect(&url.replace("http:", "ws:"), &[], options).await,
            Err(NetworkError::BudgetWait { retry_after }) if retry_after > Duration::from_secs(599))
        );
        server.join().unwrap();
    });
}
#[test]
fn successful_http_rate_error_retains_raw_and_stops_requests() {
    run(async {
        let body = r#"{"retCode":10006,"retMsg":"Too many visits!","result":{}}"#;
        let (url, server) = http_response(format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ));
        let client = client();
        let response = client.get(&url, 1, Duration::from_secs(2)).await.unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.raw_body, body.as_bytes());
        assert!(
            matches!(client.get(&url, 1, Duration::from_millis(20)).await,
            Err(NetworkError::BudgetWait { retry_after }) if retry_after > Duration::from_secs(59))
        );
        server.join().unwrap();
    });
}
#[test]
fn ban_without_retry_after_waits_three_days() {
    run(async {
        let (url, server) = http_response(
            "HTTP/1.1 418 Banned\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
        );
        let client = client();
        assert_eq!(
            client
                .get(&url, 1, Duration::from_secs(2))
                .await
                .unwrap()
                .status,
            418
        );
        assert!(
            matches!(client.get(&url, 1, Duration::from_millis(20)).await,
            Err(NetworkError::BudgetWait { retry_after }) if retry_after > Duration::from_secs(259199))
        );
        server.join().unwrap();
    });
}
#[test]
fn rest_throttle_prevents_websocket_reconnect() {
    run(async {
        let (url, server) = http_response("HTTP/1.1 429 Slow\r\nRetry-After: 600\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into());
        let client = client();
        assert_eq!(
            client
                .get(&url, 1, Duration::from_secs(2))
                .await
                .unwrap()
                .status,
            429
        );
        let options = StreamOptions {
            lifetime: Duration::from_millis(20),
            ..Default::default()
        };
        assert!(
            matches!(client.connect(&url.replace("http:", "ws:"), &[], options).await,
            Err(NetworkError::BudgetWait { retry_after }) if retry_after > Duration::from_secs(599))
        );
        server.join().unwrap();
    });
}

#[test]
fn throttle_reset_timestamp_extends_body_error_cooldown() {
    run(async {
        let body = r#"{"retCode":10006,"retMsg":"Too many visits!"}"#;
        let reset = chrono::Utc::now().timestamp_millis() + 120_000;
        let (url, server) = http_response(format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nX-Bapi-Limit-Reset-Timestamp: {reset}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ));
        let client = client();
        client.get(&url, 1, Duration::from_secs(2)).await.unwrap();
        assert!(client.cooldown_remaining().await > Duration::from_secs(119));
        server.join().unwrap();
    });
}

#[test]
fn restored_cooldown_is_shared_monotonic_and_expires() {
    run(async {
        let expiring = client();
        let client = client();
        let shared = client.clone();
        client.defer_for(Duration::from_secs(600)).await;
        shared.defer_for(Duration::ZERO).await;
        let (url, server) = http_response(
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
        );
        assert!(matches!(
            shared.get(&url, 1, Duration::from_millis(10)).await,
            Err(NetworkError::BudgetWait { .. })
        ));
        // A separate short restored cooldown must expire, not latch permanently.
        expiring.defer_for(Duration::from_millis(10)).await;
        expiring.get(&url, 1, Duration::from_secs(2)).await.unwrap();
        assert!(expiring.cooldown_remaining().await.is_zero());
        server.join().unwrap();
        client.defer_for(Duration::from_secs(8 * 86400)).await;
        shared.defer_for(Duration::from_secs(1)).await;
        assert!(shared.cooldown_remaining().await > Duration::from_secs(8 * 86400 - 1));
        assert!(
            matches!(shared.get(&url, 1, Duration::from_millis(10)).await,
            Err(NetworkError::BudgetWait { retry_after }) if retry_after > Duration::from_secs(8 * 86400 - 1))
        );
        client.defer_for(Duration::MAX).await;
        assert_eq!(shared.cooldown_remaining().await, Duration::MAX);
        assert!(matches!(
            shared.get(&url, 1, Duration::from_millis(10)).await,
            Err(NetworkError::BudgetWait {
                retry_after: Duration::MAX
            })
        ));
    });
}

#[test]
fn websocket_handshake_throttle_blocks_rest_and_returns_effective_delay() {
    run(async {
        // The handshake is an HTTP GET; this loopback fixture intentionally refuses it.
        let (url, server) = http_response(
            "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
        );
        let client = client();
        assert!(
            matches!(client.connect(&url.replace("http:", "ws:"), &[], StreamOptions::default()).await,
            Err(NetworkError::Http { status: 403, retry_after: Some(delay) }) if delay > Duration::from_secs(599))
        );
        assert!(
            matches!(client.get(&url, 1, Duration::from_millis(10)).await,
            Err(NetworkError::BudgetWait { retry_after }) if retry_after > Duration::from_secs(599))
        );
        server.join().unwrap();
    });
}
