use super::*;

use crate::ClockStatus;

fn time(seconds: i64, nanos: u32) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, nanos).expect("valid UTC fixture time")
}

fn input() -> HealthInput {
    HealthInput {
        format: HEALTH_FORMAT.into(),
        status: HealthStatus::Healthy,
        reported_at: time(100, 123_456_789),
        valid_until: time(101, 123_456_789),
        last_forecast_at: Some(time(99, 999_999_999)),
        required_sources: vec![RequiredSourceHealth {
            source_id: "spot-btcusdt".into(),
            status: HealthStatus::Unknown,
            age: Some(LocalSourceAge {
                monotonic_origin_id: "collector-run-01".into(),
                elapsed_ns: 0,
            }),
        }],
        model_versions: vec!["model-a".into(), "model-b".into()],
        data_quality: ForecastDataQuality {
            clock_status: ClockStatus::Unknown,
            flags: vec!["unverified_continuity".into()],
        },
    }
}

fn json_value(record: &HealthInput) -> serde_json::Value {
    serde_json::to_value(record).expect("serialize input fixture")
}

#[test]
fn valid_record_roundtrips_without_promoting_declarations() {
    let mut record = input();
    record.status = HealthStatus::Unknown;
    record.required_sources[0].status = HealthStatus::Unhealthy;
    record.required_sources[0].age = None;
    record.data_quality.clock_status = ClockStatus::Uncertain;
    record.required_sources.push(RequiredSourceHealth {
        source_id: "depth-btcusdt".into(),
        status: HealthStatus::Healthy,
        age: Some(LocalSourceAge {
            monotonic_origin_id: "another-local-origin".into(),
            elapsed_ns: u64::MAX,
        }),
    });
    let original = record.clone();
    let health = PredictorHealth::new(record).expect("structurally valid health record");

    assert_eq!(health.record(), &original);
    assert_eq!(health.record().status, HealthStatus::Unknown);
    assert_eq!(
        health.record().required_sources[0].status,
        HealthStatus::Unhealthy
    );
    assert_eq!(health.record().required_sources[0].age, None);
    assert_eq!(
        health.record().required_sources[1].age,
        Some(LocalSourceAge {
            monotonic_origin_id: "another-local-origin".into(),
            elapsed_ns: u64::MAX,
        })
    );
    assert_eq!(
        health.record().data_quality.clock_status,
        ClockStatus::Uncertain
    );

    let bytes = health.to_json().expect("serialize validated health");
    let restored = PredictorHealth::from_json(&bytes).expect("deserialize health");
    assert_eq!(restored.record(), &original);
    assert_eq!(
        serde_json::to_value(&restored).expect("serialize wrapper"),
        json_value(&original)
    );
}

#[test]
fn freshness_uses_half_open_declared_utc_interval() {
    let health = PredictorHealth::new(input()).expect("health");
    let start = health.record().reported_at;
    let end = health.record().valid_until;

    assert_eq!(freshness_at(None, start), HeartbeatFreshness::Missing);
    assert_eq!(
        freshness_at(Some(&health), start - chrono::Duration::nanoseconds(1)),
        HeartbeatFreshness::Future
    );
    assert_eq!(
        freshness_at(Some(&health), start),
        HeartbeatFreshness::DeclaredUnexpired
    );
    assert_eq!(
        freshness_at(Some(&health), start + chrono::Duration::nanoseconds(1)),
        HeartbeatFreshness::DeclaredUnexpired
    );
    assert_eq!(
        freshness_at(Some(&health), end - chrono::Duration::nanoseconds(1)),
        HeartbeatFreshness::DeclaredUnexpired
    );
    assert_eq!(
        freshness_at(Some(&health), end),
        HeartbeatFreshness::Expired
    );
    assert_eq!(
        freshness_at(Some(&health), end + chrono::Duration::nanoseconds(1)),
        HeartbeatFreshness::Expired
    );

    let mut zero_interval = input();
    zero_interval.valid_until = zero_interval.reported_at;
    let zero_interval = PredictorHealth::new(zero_interval).expect("equal interval is valid");
    assert_eq!(
        freshness_at(Some(&zero_interval), zero_interval.record().reported_at),
        HeartbeatFreshness::Expired
    );
}

#[test]
fn strict_json_requires_nullable_fields_and_rejects_open_or_duplicate_fields() {
    let bytes = serde_json::to_vec(&input()).expect("fixture JSON");
    assert_eq!(
        PredictorHealth::from_json(&bytes)
            .expect("valid strict JSON")
            .record(),
        &input()
    );

    let text = std::str::from_utf8(&bytes).expect("UTF-8 JSON");
    let duplicate_root = text.replace(
        "\"format\":\"predictor-health-record-v1\"",
        "\"format\":\"predictor-health-record-v1\",\"format\":\"predictor-health-record-v1\"",
    );
    assert_eq!(
        PredictorHealth::from_json(duplicate_root.as_bytes()).err(),
        Some(PredictorHealthError::MalformedJson)
    );

    let unknown_root = text.replace(
        "\"format\":\"predictor-health-record-v1\"",
        "\"unexpected\":true,\"format\":\"predictor-health-record-v1\"",
    );
    assert_eq!(
        PredictorHealth::from_json(unknown_root.as_bytes()).err(),
        Some(PredictorHealthError::MalformedJson)
    );

    let duplicate_source = text.replace(
        "\"source_id\":\"spot-btcusdt\"",
        "\"source_id\":\"spot-btcusdt\",\"source_id\":\"spot-btcusdt\"",
    );
    assert_eq!(
        PredictorHealth::from_json(duplicate_source.as_bytes()).err(),
        Some(PredictorHealthError::MalformedJson)
    );

    let unknown_age = text.replace("\"elapsed_ns\":0", "\"elapsed_ns\":0,\"future\":true");
    assert_eq!(
        PredictorHealth::from_json(unknown_age.as_bytes()).err(),
        Some(PredictorHealthError::MalformedJson)
    );

    let mut missing_last: serde_json::Value = serde_json::from_slice(&bytes).expect("value");
    missing_last
        .as_object_mut()
        .expect("record object")
        .remove("last_forecast_at");
    assert_eq!(
        PredictorHealth::from_json(
            &serde_json::to_vec(&missing_last).expect("serialize missing last time")
        )
        .err(),
        Some(PredictorHealthError::MalformedJson)
    );

    let mut missing_age: serde_json::Value = serde_json::from_slice(&bytes).expect("value");
    missing_age["required_sources"][0]
        .as_object_mut()
        .expect("source object")
        .remove("age");
    assert_eq!(
        PredictorHealth::from_json(
            &serde_json::to_vec(&missing_age).expect("serialize missing age")
        )
        .err(),
        Some(PredictorHealthError::MalformedJson)
    );
}

#[test]
fn explicit_bounds_uniqueness_and_input_times_fail_closed() {
    let original = input();
    let oversized_sources = HealthInput {
        required_sources: (0..=MAX_REQUIRED_SOURCES)
            .map(|index| RequiredSourceHealth {
                source_id: format!("source-{index}"),
                status: HealthStatus::Unknown,
                age: None,
            })
            .collect(),
        ..original.clone()
    };
    assert_eq!(
        PredictorHealth::new(oversized_sources).err(),
        Some(PredictorHealthError::TooManyRequiredSources)
    );
    let oversized_models = HealthInput {
        model_versions: (0..=MAX_MODEL_VERSIONS)
            .map(|index| format!("model-{index}"))
            .collect(),
        ..original.clone()
    };
    assert_eq!(
        PredictorHealth::new(oversized_models).err(),
        Some(PredictorHealthError::TooManyModelVersions)
    );

    let duplicate_source = HealthInput {
        required_sources: vec![original.required_sources[0].clone(); 2],
        ..original.clone()
    };
    assert_eq!(
        PredictorHealth::new(duplicate_source).err(),
        Some(PredictorHealthError::DuplicateSourceId)
    );
    let duplicate_model = HealthInput {
        model_versions: vec!["same".into(), "same".into()],
        ..original.clone()
    };
    assert_eq!(
        PredictorHealth::new(duplicate_model).err(),
        Some(PredictorHealthError::DuplicateModelVersion)
    );

    let invalid_identity = HealthInput {
        model_versions: vec![" ".into()],
        ..original.clone()
    };
    assert_eq!(
        PredictorHealth::new(invalid_identity).err(),
        Some(PredictorHealthError::InvalidIdentity)
    );
    let long_origin = HealthInput {
        required_sources: vec![RequiredSourceHealth {
            age: Some(LocalSourceAge {
                monotonic_origin_id: "x".repeat(129),
                elapsed_ns: u64::MAX,
            }),
            ..original.required_sources[0].clone()
        }],
        ..original.clone()
    };
    assert_eq!(
        PredictorHealth::new(long_origin).err(),
        Some(PredictorHealthError::InvalidIdentity)
    );
    assert_eq!(
        original.required_sources[0]
            .age
            .as_ref()
            .unwrap()
            .elapsed_ns,
        0
    );

    let at_limits = HealthInput {
        required_sources: (0..MAX_REQUIRED_SOURCES)
            .map(|index| RequiredSourceHealth {
                source_id: format!("{}s{index:09}", "x".repeat(118)),
                status: HealthStatus::Unknown,
                age: None,
            })
            .collect(),
        model_versions: (0..MAX_MODEL_VERSIONS)
            .map(|index| format!("model-{index}"))
            .collect(),
        ..original.clone()
    };
    assert!(PredictorHealth::new(at_limits).is_ok());

    let too_many_flags = HealthInput {
        data_quality: ForecastDataQuality {
            clock_status: ClockStatus::Unknown,
            flags: vec!["flag".into(); 65],
        },
        ..original.clone()
    };
    assert_eq!(
        PredictorHealth::new(too_many_flags).err(),
        Some(PredictorHealthError::InvalidDataQuality)
    );
    let invalid_flag = HealthInput {
        data_quality: ForecastDataQuality {
            clock_status: ClockStatus::Unknown,
            flags: vec![" ".into()],
        },
        ..original.clone()
    };
    assert_eq!(
        PredictorHealth::new(invalid_flag).err(),
        Some(PredictorHealthError::InvalidDataQuality)
    );

    let bad_validity = HealthInput {
        valid_until: original.reported_at - chrono::Duration::nanoseconds(1),
        ..original.clone()
    };
    assert_eq!(
        PredictorHealth::new(bad_validity).err(),
        Some(PredictorHealthError::InvalidTime)
    );
    let future_forecast = HealthInput {
        last_forecast_at: Some(original.reported_at + chrono::Duration::nanoseconds(1)),
        ..original.clone()
    };
    assert_eq!(
        PredictorHealth::new(future_forecast).err(),
        Some(PredictorHealthError::InvalidTime)
    );
    assert!(PredictorHealth::new(original.clone()).is_ok());
}

#[test]
fn bounded_json_and_empty_startup_record_are_supported() {
    let empty_startup = HealthInput {
        status: HealthStatus::Unknown,
        last_forecast_at: None,
        required_sources: Vec::new(),
        model_versions: Vec::new(),
        ..input()
    };
    let serialized = PredictorHealth::new(empty_startup.clone())
        .expect("empty startup health record")
        .to_json()
        .expect("serialize startup health");
    let mut at_limit = serialized.clone();
    at_limit.resize(MAX_HEALTH_JSON_BYTES, b' ');
    assert!(PredictorHealth::from_json(&at_limit).is_ok());
    at_limit.push(b' ');
    assert_eq!(
        PredictorHealth::from_json(&at_limit).err(),
        Some(PredictorHealthError::InputTooLarge)
    );
    assert_eq!(
        PredictorHealth::from_json(&serialized)
            .expect("empty startup parses")
            .record(),
        &empty_startup
    );
}
