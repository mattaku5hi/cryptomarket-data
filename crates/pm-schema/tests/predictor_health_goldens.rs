#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used
)]

use chrono::{DateTime, Duration, Utc};
use pm_schema::predictor_health::{HealthInput, HeartbeatFreshness, PredictorHealth, freshness_at};
use serde_json::Value;

fn freshness(value: &str) -> HeartbeatFreshness {
    match value {
        "Missing" => HeartbeatFreshness::Missing,
        "Future" => HeartbeatFreshness::Future,
        "Expired" => HeartbeatFreshness::Expired,
        "DeclaredUnexpired" => HeartbeatFreshness::DeclaredUnexpired,
        other => panic!("unknown fixture heartbeat status: {other}"),
    }
}

fn timestamp(value: &Value) -> DateTime<Utc> {
    serde_json::from_value(value.clone()).expect("fixture UTC timestamp")
}

#[test]
fn predictor_health_wire_and_freshness_match_synthetic_golden() {
    let reference: Value = serde_json::from_str(include_str!(
        "fixtures/predictor_health_2026_10_11/reference.json"
    ))
    .expect("synthetic PredictorHealth golden");
    assert_eq!(
        reference["format"],
        "synthetic-predictor-health-reference-v1"
    );
    assert_eq!(reference["wire_format"], "predictor-health-record-v1");
    assert!(
        reference["provenance"]["synthetic_metadata_only"]
            .as_bool()
            .expect("synthetic-only declaration")
    );
    assert!(
        reference["provenance"]["no_source_or_clock_qualification"]
            .as_bool()
            .expect("source/clock limitation declaration")
    );
    assert!(
        reference["provenance"]["no_forecast_or_execution_eligibility_claim"]
            .as_bool()
            .expect("eligibility limitation declaration")
    );

    let cases = reference["cases"].as_array().expect("fixture cases");
    assert_eq!(cases.len(), 4);
    let mut saw_zero_age = false;
    let mut saw_missing_age = false;
    let mut saw_uncertain_clock = false;
    let mut saw_healthy_clock = false;

    for case in cases {
        let name = case["name"].as_str().expect("case name");
        let expected_input: HealthInput = serde_json::from_value(case["record"].clone())
            .expect("fixture typed PredictorHealth input");
        let wire = hex::decode(case["compact_json_hex"].as_str().expect("compact JSON hex"))
            .expect("valid compact JSON hex");
        assert_eq!(
            serde_json::to_vec(&expected_input).expect("typed input JSON"),
            wire
        );

        let health = PredictorHealth::from_json(&wire).expect("strict bounded PredictorHealth");
        assert_eq!(health.record(), &expected_input, "{name}");
        let serialized = health
            .to_json()
            .expect("serialize validated PredictorHealth");
        assert_eq!(serialized, wire, "{name}");
        let roundtrip = PredictorHealth::from_json(&serialized).expect("strict JSON roundtrip");
        assert_eq!(roundtrip.record(), &expected_input, "{name}");

        if expected_input.data_quality.clock_status == pm_schema::ClockStatus::Uncertain {
            saw_uncertain_clock = true;
        }
        if matches!(
            expected_input.data_quality.clock_status,
            pm_schema::ClockStatus::Healthy { .. }
        ) {
            saw_healthy_clock = true;
        }
        for source in &expected_input.required_sources {
            match &source.age {
                Some(age) if age.elapsed_ns == 0 => saw_zero_age = true,
                None => saw_missing_age = true,
                _ => {}
            }
        }

        let probes = case["freshness"].as_array().expect("freshness probes");
        let start = expected_input.reported_at;
        let end = expected_input.valid_until;
        let expected_times = if probes.len() == 6 {
            vec![
                start - Duration::nanoseconds(1),
                start,
                start + Duration::nanoseconds(1),
                end - Duration::nanoseconds(1),
                end,
                end + Duration::nanoseconds(1),
            ]
        } else if start == end {
            vec![
                start - Duration::nanoseconds(1),
                start,
                start + Duration::nanoseconds(1),
            ]
        } else {
            vec![start - Duration::nanoseconds(1), start, end]
        };
        assert_eq!(probes.len(), expected_times.len(), "{name} probe count");
        for (probe, expected_time) in probes.iter().zip(expected_times) {
            let now = timestamp(&probe["now"]);
            assert_eq!(now, expected_time, "{name} exact nanosecond edge");
            assert_eq!(
                freshness_at(Some(&health), now),
                freshness(probe["expected"].as_str().expect("freshness expected")),
                "{name}"
            );
        }

        if name == "ordered_sources_and_models" {
            assert_eq!(expected_input.required_sources.len(), 2);
            assert_eq!(expected_input.required_sources[0].source_id, "binance-spot");
            assert_eq!(expected_input.required_sources[1].source_id, "binance-usdm");
            assert_eq!(
                expected_input.model_versions,
                [
                    "toy-residual-v2".to_owned(),
                    "toy-independent-v1".to_owned()
                ]
            );
            assert!(
                expected_input.required_sources[0]
                    .age
                    .as_ref()
                    .is_some_and(|age| age.elapsed_ns == 0)
            );
            assert!(expected_input.required_sources[1].age.is_none());
        }
    }

    assert!(saw_zero_age && saw_missing_age);
    assert!(saw_uncertain_clock && saw_healthy_clock);
    for probe in reference["missing_heartbeat_freshness"]
        .as_array()
        .expect("missing heartbeat probes")
    {
        assert_eq!(
            freshness_at(None, timestamp(&probe["now"])),
            freshness(
                probe["expected"]
                    .as_str()
                    .expect("missing freshness expected")
            )
        );
    }
}
