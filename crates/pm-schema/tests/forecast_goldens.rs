#![allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]

use pm_schema::{MarketSpec, forecast::ProbabilityForecast};
use serde_json::Value;

#[test]
fn bound_structural_records_match_independent_json_and_content_digests() {
    let reference: Value =
        serde_json::from_str(include_str!("fixtures/forecast_2026_10_10/reference.json")).unwrap();
    assert_eq!(reference["genuine_market_data"], false);
    assert_eq!(reference["source_or_clock_admission"], false);
    let cases = reference["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 6);
    let mut hashes = Vec::new();
    for case in cases {
        let market: MarketSpec = serde_json::from_value(case["market"].clone()).unwrap();
        let wire = serde_json::to_vec(&case["input"]).unwrap();
        let record = ProbabilityForecast::from_json(&wire, &market).unwrap();
        let serialized = serde_json::to_string(&record).unwrap();
        assert_eq!(
            serialized,
            case["compact_json"].as_str().unwrap(),
            "{}",
            case["name"]
        );
        assert_eq!(
            record.content_sha256().unwrap(),
            case["content_sha256"].as_str().unwrap()
        );
        let roundtrip = ProbabilityForecast::from_json(serialized.as_bytes(), &market).unwrap();
        assert_eq!(roundtrip, record);
        assert_eq!(
            record.record().data_quality.flags,
            case["canonical_record"]["data_quality"]["flags"]
                .as_array()
                .unwrap()
                .iter()
                .map(|flag| flag.as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        );
        if case["name"] == "negative_zero" {
            assert_eq!(record.record().p_up.to_bits(), 0);
            assert_eq!(record.record().q_market_up.unwrap().to_bits(), 0);
        }
        hashes.push(record.content_sha256().unwrap());
    }
    // Same caller ID can be retried; changed semantic content must be distinguishable.
    assert_eq!(
        cases[0]["input"]["forecast_id"],
        cases[5]["input"]["forecast_id"]
    );
    assert_ne!(hashes[0], hashes[5]);
}
