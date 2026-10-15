//! Byte-bounded JSON with recursive duplicate-key rejection; the byte bound is not an RSS bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    InvalidBounds,
    Limit,
    Malformed,
}

/// Validates complete JSON, including unused nested fields, under serde_json's recursion ceiling.
pub fn validate(raw: &[u8], max_bytes: usize) -> Result<(), Error> {
    if max_bytes == 0 {
        return Err(Error::InvalidBounds);
    }
    if raw.len() > max_bytes {
        return Err(Error::Limit);
    }
    reject_duplicates(raw).map_err(|_| Error::Malformed)
}

/// Validates before constructing a value in a second pass; preserves JSON types and decimal strings.
pub fn parse(raw: &[u8], max_bytes: usize) -> Result<serde_json::Value, Error> {
    validate(raw, max_bytes)?;
    serde_json::from_slice(raw).map_err(|_| Error::Malformed)
}

use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use std::collections::HashSet;
use std::fmt;

fn reject_duplicates(raw: &[u8]) -> Result<(), ()> {
    let mut deserializer = serde_json::Deserializer::from_slice(raw);
    UnitSeed.deserialize(&mut deserializer).map_err(|_| ())?;
    deserializer.end().map_err(|_| ())
}
struct UnitSeed;
impl<'de> DeserializeSeed<'de> for UnitSeed {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(UnitVisitor)
    }
}
struct UnitVisitor;
impl<'de> Visitor<'de> for UnitVisitor {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JSON value with unique object keys")
    }
    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<(), E> {
        Ok(())
    }
    fn visit_string<E: serde::de::Error>(self, _: String) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_none<E: serde::de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        UnitSeed.deserialize(d)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while seq.next_element_seed(UnitSeed)?.is_some() {}
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let mut keys = HashSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key) {
                return Err(serde::de::Error::custom("duplicate object key"));
            }
            map.next_value_seed(UnitSeed)?;
        }
        Ok(())
    }
}
