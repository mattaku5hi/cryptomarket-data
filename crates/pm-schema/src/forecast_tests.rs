use super::*;
use chrono::TimeZone;

fn ts(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).single().expect("timestamp")
}

fn market() -> MarketSpec {
    let rule_text = "synthetic forecast rule".to_owned();
    MarketSpec {
        market_id: "market-5m".to_owned(),
        condition_id: format!("0x{}", "ab".repeat(32)),
        up_token_id: "1".to_owned(),
        down_token_id: "2".to_owned(),
        horizon: Horizon::FiveMinutes,
        listed_at: ts(0),
        interval_start: ts(100),
        interval_end: ts(400),
        trading_close: ts(300),
        rule_hash: sha256(rule_text.as_bytes()),
        rule_text,
        resolution_source: "synthetic-source".to_owned(),
        feed_id: "synthetic-feed".to_owned(),
        window_seconds: 60,
        resolver_version: "resolver-v1".to_owned(),
        platform_regime_id: "regime-v1".to_owned(),
        first_available_at: ts(10),
    }
}

fn input() -> ForecastInput {
    ForecastInput {
        format: FORECAST_FORMAT.to_owned(),
        forecast_id: "forecast-1".to_owned(),
        market_id: "market-5m".to_owned(),
        horizon: Horizon::FiveMinutes,
        market_phase: MarketPhase::InWindow,
        predicted_at: ts(150),
        valid_until: ts(200),
        available_data_cutoff: ts(120),
        reference_age: LocalReferenceAge {
            monotonic_origin_id: "process-epoch-1".to_owned(),
            elapsed_ns: 10_000,
        },
        p_up: 0.25,
        p_down: 0.75,
        q_market_up: Some(0.5),
        model_version: "model-v1".to_owned(),
        calibration_version: "calibration-v1".to_owned(),
        feature_schema_hash: "a".repeat(64),
        resolver_version: "resolver-v1".to_owned(),
        platform_regime_id: "regime-v1".to_owned(),
        data_quality: ForecastDataQuality {
            clock_status: ClockStatus::Unknown,
            flags: vec!["opaque-flag".to_owned(), "opaque-flag".to_owned()],
        },
    }
}

fn encoded_input(record: &ForecastInput) -> Vec<u8> {
    serde_json::to_vec(record).expect("record fixture serialization")
}

#[test]
fn validated_forecast_roundtrips_and_hashes_versioned_content() {
    let input = input();
    let forecast = ProbabilityForecast::new(input, &market()).expect("forecast");
    let bytes = serde_json::to_vec(&forecast).expect("forecast serialization");
    let parsed = ProbabilityForecast::from_json(&bytes, &market()).expect("forecast parse");

    assert_eq!(parsed, forecast);
    assert_eq!(
        parsed.record().data_quality.clock_status,
        ClockStatus::Unknown
    );
    assert_eq!(
        parsed.record().data_quality.flags,
        vec!["opaque-flag", "opaque-flag"]
    );
    let mut expected_content = CONTENT_HASH_PREFIX.to_vec();
    expected_content.extend_from_slice(&serde_json::to_vec(parsed.record()).expect("record JSON"));
    assert_eq!(parsed.content_sha256(), Ok(sha256(&expected_content)));
}

#[test]
fn construction_checks_full_market_identity_and_market_validity() {
    let mut bad_binding = input();
    bad_binding.resolver_version = "other-resolver".to_owned();
    assert_eq!(
        ProbabilityForecast::new(bad_binding, &market()),
        Err(ForecastError::MarketBinding)
    );

    let mut invalid_market = market();
    invalid_market.rule_hash = "0".repeat(64);
    assert_eq!(
        ProbabilityForecast::new(input(), &invalid_market),
        Err(ForecastError::InvalidMarket)
    );
}

#[test]
fn metadata_availability_and_time_ordering_fail_closed() {
    let mut exact_first_available = input();
    exact_first_available.available_data_cutoff = ts(10);
    exact_first_available.predicted_at = ts(10);
    exact_first_available.valid_until = ts(10);
    exact_first_available.market_phase = MarketPhase::PreWindow;
    assert!(ProbabilityForecast::new(exact_first_available, &market()).is_ok());

    let mut equal_cutoff_and_validity = input();
    equal_cutoff_and_validity.available_data_cutoff = equal_cutoff_and_validity.predicted_at;
    equal_cutoff_and_validity.valid_until = equal_cutoff_and_validity.predicted_at;
    assert!(ProbabilityForecast::new(equal_cutoff_and_validity, &market()).is_ok());

    let mut unavailable = input();
    unavailable.available_data_cutoff = ts(9);
    assert_eq!(
        ProbabilityForecast::new(unavailable, &market()),
        Err(ForecastError::MarketMetadataUnavailable)
    );

    let mut cutoff_after_prediction = input();
    cutoff_after_prediction.available_data_cutoff = ts(151);
    assert_eq!(
        ProbabilityForecast::new(cutoff_after_prediction, &market()),
        Err(ForecastError::InvalidTime)
    );

    let mut expires_before_prediction = input();
    expires_before_prediction.valid_until = ts(149);
    assert_eq!(
        ProbabilityForecast::new(expires_before_prediction, &market()),
        Err(ForecastError::InvalidTime)
    );

    let mut expires_after_close = input();
    expires_after_close.valid_until = ts(301);
    assert_eq!(
        ProbabilityForecast::new(expires_after_close, &market()),
        Err(ForecastError::InvalidTime)
    );
}

#[test]
fn phase_must_match_and_close_boundary_is_structurally_valid() {
    let mut wrong_phase = input();
    wrong_phase.market_phase = MarketPhase::PreWindow;
    assert_eq!(
        ProbabilityForecast::new(wrong_phase, &market()),
        Err(ForecastError::PhaseMismatch)
    );

    let mut close = input();
    close.market_phase = MarketPhase::Closed;
    close.predicted_at = ts(300);
    close.valid_until = ts(300);
    let forecast = ProbabilityForecast::new(close, &market()).expect("inclusive close record");
    assert_eq!(forecast.record().market_phase, MarketPhase::Closed);
}

#[test]
fn probabilities_require_finite_range_and_exact_complements_then_canonicalize_zero() {
    let mut negative_zero = input();
    negative_zero.p_up = -0.0;
    negative_zero.p_down = 1.0;
    negative_zero.q_market_up = Some(-0.0);
    let forecast = ProbabilityForecast::new(negative_zero, &market()).expect("zero canonicalized");
    assert_eq!(forecast.record().p_up.to_bits(), 0.0_f64.to_bits());
    assert_eq!(
        forecast.record().q_market_up.expect("q").to_bits(),
        0.0_f64.to_bits()
    );

    for bad in [f64::NAN, f64::INFINITY, -0.01, 1.01] {
        let mut invalid = input();
        invalid.q_market_up = Some(bad);
        assert_eq!(
            ProbabilityForecast::new(invalid, &market()),
            Err(ForecastError::InvalidProbability)
        );
    }
    let mut invalid_pair = input();
    invalid_pair.p_down = 0.5;
    assert_eq!(
        ProbabilityForecast::new(invalid_pair, &market()),
        Err(ForecastError::InvalidProbabilityComplement)
    );
}

#[test]
fn identities_schema_and_opaque_quality_fields_are_bounded() {
    let mut invalid_identity = input();
    invalid_identity.model_version = " ".to_owned();
    assert_eq!(
        ProbabilityForecast::new(invalid_identity, &market()),
        Err(ForecastError::InvalidIdentity)
    );

    let mut invalid_schema = input();
    invalid_schema.feature_schema_hash = "A".repeat(64);
    assert_eq!(
        ProbabilityForecast::new(invalid_schema, &market()),
        Err(ForecastError::InvalidFeatureSchemaHash)
    );

    let mut too_many_flags = input();
    too_many_flags.data_quality.flags = vec!["f".to_owned(); 65];
    assert_eq!(
        ProbabilityForecast::new(too_many_flags, &market()),
        Err(ForecastError::InvalidDataQuality)
    );

    let escaped = "x\u{1}".repeat(64);
    let mut bounded = input();
    let mut market = market();
    market.market_id = escaped.clone();
    market.resolver_version = escaped.clone();
    market.platform_regime_id = escaped.clone();
    bounded.market_id = escaped.clone();
    bounded.forecast_id = escaped.clone();
    bounded.model_version = escaped.clone();
    bounded.calibration_version = escaped.clone();
    bounded.resolver_version = escaped.clone();
    bounded.platform_regime_id = escaped.clone();
    bounded.reference_age.monotonic_origin_id = escaped.clone();
    bounded.data_quality.flags = vec![escaped; 64];
    let forecast = ProbabilityForecast::new(bounded, &market).expect("bounded metadata");
    let serialized = serde_json::to_vec(&forecast).expect("escaped metadata serialization");
    assert!(serialized.len() > 16 * 1024);
    assert!(serialized.len() <= MAX_FORECAST_JSON_BYTES);
    assert!(ProbabilityForecast::from_json(&serialized, &market).is_ok());
}

#[test]
fn strict_json_requires_nullable_q_and_rejects_unknown_duplicate_and_integer_overflow() {
    let market = market();
    let bytes = encoded_input(&input());
    assert!(ProbabilityForecast::from_json(&bytes, &market).is_ok());

    let mut missing_q: serde_json::Value = serde_json::from_slice(&bytes).expect("input JSON");
    missing_q
        .as_object_mut()
        .expect("object")
        .remove("q_market_up");
    assert_eq!(
        ProbabilityForecast::from_json(&serde_json::to_vec(&missing_q).expect("JSON"), &market),
        Err(ForecastError::MalformedJson)
    );

    let mut null_q = input();
    null_q.q_market_up = None;
    assert!(ProbabilityForecast::new(null_q.clone(), &market).is_ok());
    let mut null_q_json = serde_json::to_value(&null_q).expect("JSON value");
    null_q_json["q_market_up"] = serde_json::Value::Null;
    assert!(
        ProbabilityForecast::from_json(&serde_json::to_vec(&null_q_json).expect("JSON"), &market)
            .is_ok()
    );

    let mut unknown = String::from_utf8(bytes.clone()).expect("UTF8");
    unknown.pop();
    unknown.push_str(r#", "unknown":true}"#);
    assert_eq!(
        ProbabilityForecast::from_json(unknown.as_bytes(), &market),
        Err(ForecastError::MalformedJson)
    );

    let mut duplicate = String::from_utf8(bytes.clone()).expect("UTF8");
    duplicate.pop();
    duplicate.push_str(r#", "forecast_id":"other"}"#);
    assert_eq!(
        ProbabilityForecast::from_json(duplicate.as_bytes(), &market),
        Err(ForecastError::MalformedJson)
    );

    let max_age = u64::MAX.to_string();
    let bounded_age = String::from_utf8(bytes.clone())
        .expect("UTF8")
        .replace("10000", &max_age);
    assert!(ProbabilityForecast::from_json(bounded_age.as_bytes(), &market).is_ok());
    let overflowing_age = bounded_age.replace(&max_age, "18446744073709551616");
    assert_eq!(
        ProbabilityForecast::from_json(overflowing_age.as_bytes(), &market),
        Err(ForecastError::MalformedJson)
    );

    let mut extreme_clock = input();
    extreme_clock.data_quality.clock_status = ClockStatus::Healthy {
        offset_nanos: i64::MIN,
        uncertainty_nanos: u64::MAX,
    };
    let extreme_clock_json = encoded_input(&extreme_clock);
    let decoded = ProbabilityForecast::from_json(&extreme_clock_json, &market)
        .expect("typed clock bounds remain annotations");
    assert_eq!(
        decoded.record().data_quality.clock_status,
        ClockStatus::Healthy {
            offset_nanos: i64::MIN,
            uncertainty_nanos: u64::MAX,
        }
    );

    let mut unknown_clock: serde_json::Value =
        serde_json::from_slice(&extreme_clock_json).expect("clock fixture JSON");
    unknown_clock["data_quality"]["clock_status"]["Healthy"]["extra"] = serde_json::json!(true);
    assert_eq!(
        ProbabilityForecast::from_json(
            &serde_json::to_vec(&unknown_clock).expect("unknown clock JSON"),
            &market
        ),
        Err(ForecastError::MalformedJson)
    );

    let mut duplicate_clock = String::from_utf8(extreme_clock_json.clone()).expect("UTF8");
    duplicate_clock = duplicate_clock.replace(
        r#""offset_nanos":-9223372036854775808,"#,
        r#""offset_nanos":-9223372036854775808,"offset_nanos":-9223372036854775808,"#,
    );
    assert_eq!(
        ProbabilityForecast::from_json(duplicate_clock.as_bytes(), &market),
        Err(ForecastError::MalformedJson)
    );

    let mut missing_clock: serde_json::Value =
        serde_json::from_slice(&extreme_clock_json).expect("clock fixture JSON");
    missing_clock["data_quality"]["clock_status"]["Healthy"]
        .as_object_mut()
        .expect("healthy object")
        .remove("offset_nanos");
    assert_eq!(
        ProbabilityForecast::from_json(
            &serde_json::to_vec(&missing_clock).expect("missing clock JSON"),
            &market
        ),
        Err(ForecastError::MalformedJson)
    );
}

#[test]
fn raw_json_size_is_checked_before_parsing() {
    let oversized = vec![b' '; MAX_FORECAST_JSON_BYTES + 1];
    assert_eq!(
        ProbabilityForecast::from_json(&oversized, &market()),
        Err(ForecastError::InputTooLarge)
    );
}

#[test]
fn same_forecast_id_with_changed_content_gets_a_different_hash() {
    let original = ProbabilityForecast::new(input(), &market()).expect("original");
    let mut changed_input = input();
    changed_input.p_up = 0.75;
    changed_input.p_down = 0.25;
    let changed = ProbabilityForecast::new(changed_input, &market()).expect("changed");

    assert_eq!(original.record().forecast_id, changed.record().forecast_id);
    assert_ne!(original.content_sha256(), changed.content_sha256());
}
