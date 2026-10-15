#![allow(clippy::unwrap_used, clippy::indexing_slicing)]
use pm_schema::json::{Error, parse, validate};

#[test]
fn bounds_complete_input_and_nested_duplicates() {
    let raw = br#"{"amount":"123.000000000000000001","extra":[{"key":1}]}"#;
    assert_eq!(validate(raw, raw.len()), Ok(()));
    assert_eq!(validate(raw, raw.len() - 1), Err(Error::Limit));
    assert_eq!(validate(raw, 0), Err(Error::InvalidBounds));
    assert_eq!(
        parse(raw, raw.len()).unwrap()["amount"],
        "123.000000000000000001"
    );
    for invalid in [
        &br#"{"extra":[{"key":1,"key":2}]}"#[..],
        &br#"{"key":1,"\u006bey":2}"#[..],
        &b"{} {}"[..],
        &b""[..],
        &b"{\"x\":"[..],
    ] {
        assert_eq!(validate(invalid, 128), Err(Error::Malformed));
    }
}
