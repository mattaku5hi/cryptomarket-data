use super::*;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Value, json};

#[test]
fn decimal_grammar_precision_and_serde_are_exact_strings() {
    for invalid in [
        "1_000",
        "1.",
        " .1",
        ".1",
        "1.0000000000000000000",
        "0.123456789012345678901",
        "1e-3",
        "+1",
    ] {
        assert!(
            ExactDecimal::parse(invalid).is_err(),
            "accepted {invalid:?}"
        );
    }
    let n = ExactDecimal::parse("0.123456789012345678").unwrap();
    assert_eq!(
        serde_json::to_string(&n).unwrap(),
        "\"0.123456789012345678\""
    );
    assert!(serde_json::from_str::<ExactDecimal>("0.1").is_err());
    assert!(serde_json::from_str::<BinaryPrice>("\"-0.1\"").is_err());
    assert!(serde_json::from_str::<BinaryPrice>("\"1.1\"").is_err());
    assert!(serde_json::from_str::<NonnegativeShares>("\"-0.1\"").is_err());
    assert!(serde_json::from_str::<ExactDecimal>("\"1_000\"").is_err());
    assert!(ExactDecimal::parse("79228162514264337593543950336").is_err());
}

#[test]
fn reference_requires_positive_validated_values() {
    assert!(PositiveReferencePrice::new(d("0")).is_err());
    assert!(PositiveReferencePrice::new(d("-1")).is_err());
    assert_eq!(reference_comparison(p("100"), p("100")), OutcomeSide::Up);
}

#[test]
fn token_ids_are_canonical_uint256_decimal_strings() {
    let max = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    let (mut spec, _) = fixture();
    spec.up_token_id = max.into();
    assert!(spec.validate().is_ok());
    for invalid in [
        "",
        "00",
        "01",
        "-1",
        "+1",
        "1e2",
        "115792089237316195423570985008687907853269984665640564039457584007913129639936",
    ] {
        spec.up_token_id = invalid.into();
        assert!(spec.validate().is_err(), "accepted token {invalid:?}");
    }
    spec.up_token_id = "1".into();
    spec.down_token_id = "01".into();
    assert!(spec.validate().is_err());
    spec.down_token_id = "1".into();
    assert!(spec.validate().is_err());
    let mut value = serde_json::to_value(&spec).unwrap();
    value["up_token_id"] = json!(1);
    assert!(serde_json::from_value::<MarketSpec>(value).is_err());
}

#[test]
fn market_identity_interval_availability_and_rule_binding_are_validated() {
    let (spec, _) = fixture();
    assert!(spec.validate().is_ok());
    let mut bad = spec.clone();
    bad.condition_id = format!("0x{}", "AB".repeat(32));
    assert!(bad.validate().is_err());
    let mut bad = spec.clone();
    bad.condition_id = format!("0x{}", "ab".repeat(31));
    assert!(bad.validate().is_err());
    let mut bad = spec.clone();
    bad.rule_hash = "0".repeat(64);
    assert!(bad.validate().is_err());
    let mut bad = spec.clone();
    bad.interval_end = bad.interval_start + chrono::Duration::seconds(301);
    assert!(bad.validate().is_err());
    let mut bad = spec.clone();
    bad.listed_at = bad.interval_start + chrono::Duration::seconds(1);
    assert!(bad.validate().is_err());
    let mut bad = spec.clone();
    bad.trading_close = bad.interval_start;
    assert!(bad.validate().is_err());
    let mut bad = spec.clone();
    bad.trading_close = bad.interval_end + chrono::Duration::nanoseconds(1);
    assert!(bad.validate().is_err());
    let binding = SupportedRule {
        expected_rule_hash: spec.rule_hash.clone(),
        resolution_source: spec.resolution_source.clone(),
        horizon: spec.horizon,
        feed_id: spec.feed_id.clone(),
        window_seconds: 60,
        resolver_version: spec.resolver_version.clone(),
    };
    assert!(spec.validate_against(&binding).is_ok());
    for field in 0..5 {
        let mut b = binding.clone();
        match field {
            0 => b.expected_rule_hash = "wrong".into(),
            1 => b.resolution_source = "wrong".into(),
            2 => b.horizon = Horizon::FifteenMinutes,
            3 => b.feed_id = "wrong".into(),
            _ => b.resolver_version = "wrong".into(),
        }
        assert!(spec.validate_against(&b).is_err());
    }
    let mut bad = spec.clone();
    bad.window_seconds = 0;
    assert!(bad.validate_against(&binding).is_err());
}

#[test]
fn phase_is_fail_closed_for_invalid_or_not_yet_available_spec() {
    let (spec, _) = fixture();
    assert_eq!(
        spec.phase_at(ts(-2)),
        Err(SchemaError("market not listed".into()))
    );
    assert_eq!(spec.phase_at(ts(0)).unwrap(), MarketPhase::PreWindow);
    assert_eq!(spec.phase_at(ts(1)).unwrap(), MarketPhase::InWindow);
    assert_eq!(spec.phase_at(ts(300)).unwrap(), MarketPhase::Closed);
    let mut late = spec.clone();
    late.first_available_at = ts(4);
    assert!(late.phase_at(ts(3)).is_err());
    assert_eq!(late.phase_at(ts(4)).unwrap(), MarketPhase::InWindow);
    assert!(late.in_window_at(ts(3)).is_err());
    assert!(late.in_window_at(ts(4)).unwrap());
    let mut bad = spec;
    bad.down_token_id = bad.up_token_id.clone();
    assert!(bad.phase_at(ts(2)).is_err());
}

#[test]
fn market_spec_deserialization_validates_and_timestamp_is_nanosecond_exact() {
    let (spec, _) = fixture();
    let mut v = serde_json::to_value(&spec).unwrap();
    v["up_token_id"] =
        json!("12345678901234567890123456789012345678901234567890123456789012345678901234567");
    let decoded: MarketSpec = serde_json::from_value(v.clone()).unwrap();
    assert_eq!(decoded.up_token_id, v["up_token_id"].as_str().unwrap());
    v["down_token_id"] = v["up_token_id"].clone();
    assert!(serde_json::from_value::<MarketSpec>(v).is_err());
    let timestamp = DateTime::<Utc>::from_timestamp(1_700_000_000, 123_456_789).unwrap();
    assert_eq!(
        serde_json::from_str::<DateTime<Utc>>(&serde_json::to_string(&timestamp).unwrap()).unwrap(),
        timestamp
    );
}

#[test]
fn envelope_validates_identity_integrity_causality_clock_and_serde() {
    let mut e = clean_envelope();
    e.validate().unwrap();
    assert_eq!(e.source_event_time, None);
    assert_eq!(e.source_publish_time, None);
    e.visible_at(ts(2)).unwrap();
    e.source_event_time = Some(ts(0));
    e.provider_time_unit = Some("unix_ms".into());
    e.validate().unwrap();
    assert!(e.visible_at(ts(1)).is_err()); // old source event arriving later is not yet available
    e.source_event_time = Some(ts(99));
    e.clock_status = ClockStatus::Uncertain;
    e.validate().unwrap();
    assert!(e.visible_at(ts(2)).is_err());
    e.source_event_time = Some(ts(1));
    e.provider_time_unit = None;
    assert!(e.validate().is_err());
    let mut e = envelope();
    e.ingest_sequence = 0;
    assert!(e.validate().is_err());
    let mut e = envelope();
    e.source = "".into();
    assert!(e.validate().is_err());
    let mut e = envelope();
    e.schema_version = 2;
    assert!(e.validate().is_err());
    let mut e = envelope();
    e.available_at = ts(0);
    assert!(e.validate().is_err());
    let mut e = envelope();
    e.raw_payload[0] ^= 1;
    assert!(e.validate().is_err());
    let mut e = clean_envelope();
    e.kind = EventKind::Gap;
    assert!(e.visible_at(ts(2)).unwrap_err().0.contains("gap"));
    let mut e = clean_envelope();
    e.quality_flags.push("CLOCK_UNCERTAIN".into());
    assert!(e.visible_at(ts(2)).unwrap_err().0.contains("quality"));
    let mut e = clean_envelope();
    e.source_publish_time = Some(ts(1));
    e.provider_time_unit = Some("  ".into());
    assert!(e.validate().unwrap_err().0.contains("unit"));
    let e = envelope();
    let value = serde_json::to_value(&e).unwrap();
    assert_eq!(value["source_event_time"], Value::Null);
    assert_eq!(
        serde_json::from_value::<EventEnvelope>(value)
            .unwrap()
            .raw_payload,
        e.raw_payload
    );
    let mut value = serde_json::to_value(envelope()).unwrap();
    value["schema_version"] = json!(7);
    assert!(serde_json::from_value::<EventEnvelope>(value).is_err());
    let mut value = serde_json::to_value(envelope()).unwrap();
    value.as_object_mut().unwrap().remove("source");
    assert!(serde_json::from_value::<EventEnvelope>(value).is_err());
}

fn fixture() -> (MarketSpec, String) {
    let rule = "synthetic rule: final >= opening".to_string();
    (
        MarketSpec {
            market_id: "synthetic-market".into(),
            condition_id: format!("0x{}", "ab".repeat(32)),
            up_token_id: "1".into(),
            down_token_id: "2".into(),
            horizon: Horizon::FiveMinutes,
            listed_at: ts(-1),
            interval_start: ts(1),
            interval_end: ts(301),
            trading_close: ts(300),
            rule_text: rule.clone(),
            rule_hash: sha256(rule.as_bytes()),
            resolution_source: "synthetic-source".into(),
            feed_id: "synthetic-feed".into(),
            window_seconds: 60,
            resolver_version: "synthetic-v1".into(),
            platform_regime_id: "synthetic-regime".into(),
            first_available_at: ts(0),
        },
        rule,
    )
}
fn envelope() -> EventEnvelope {
    let payload = vec![0, 255, 1];
    EventEnvelope {
        schema_version: 1,
        source: "synthetic".into(),
        stream: "synthetic".into(),
        collector_id: "c".into(),
        collector_region: "test".into(),
        connection_id: "conn".into(),
        collector_session_id: "boot-1".into(),
        ingest_sequence: 1,
        source_sequence: None,
        source_event_time: None,
        source_publish_time: None,
        received_at: ts(1),
        available_at: ts(2),
        monotonic_elapsed_nanos: 1,
        provider_time_unit: None,
        kind: EventKind::Snapshot,
        raw_payload: payload.clone(),
        quality_flags: vec!["SYNTHETIC_FIXTURE".into()],
        payload_sha256: sha256(&payload),
        clock_status: ClockStatus::Unknown,
    }
}
fn clean_envelope() -> EventEnvelope {
    let mut e = envelope();
    e.clock_status = ClockStatus::Healthy {
        offset_nanos: 0,
        uncertainty_nanos: 1,
    };
    e.quality_flags.clear();
    e
}
fn d(s: &str) -> ExactDecimal {
    ExactDecimal::parse(s).unwrap()
}
fn p(s: &str) -> PositiveReferencePrice {
    PositiveReferencePrice::new(d(s)).unwrap()
}
fn ts(s: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(s, 0).single().unwrap()
}
