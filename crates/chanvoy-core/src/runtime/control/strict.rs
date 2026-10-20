use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::{Map, Value};
use std::fmt;

struct Strict(Value);
impl<'de> Deserialize<'de> for Strict {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct StrictVisitor;
        impl<'de> Visitor<'de> for StrictVisitor {
            type Value = Strict;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("strict JSON")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Strict, E> {
                Ok(Strict(Value::Bool(v)))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_f64<E: de::Error>(self, _: f64) -> Result<Strict, E> {
                Err(E::custom("numeric token was not normalized"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Strict, E> {
                Ok(Strict(Value::String(v.into())))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Strict, E> {
                Ok(Strict(Value::String(v)))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Strict, E> {
                Ok(Strict(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Strict, A::Error> {
                let mut values = Vec::new();
                while let Some(Strict(value)) = seq.next_element()? {
                    values.push(value);
                }
                Ok(Strict(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Strict, A::Error> {
                let mut values = Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom("duplicate JSON key"));
                    }
                    let Strict(value) = map.next_value()?;
                    values.insert(key, value);
                }
                Ok(Strict(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(StrictVisitor)
    }
}

pub(super) fn parse(text: &str) -> Result<Value, ()> {
    let normalized = normalize_numbers(text)?;
    let mut deserializer = serde_json::Deserializer::from_str(&normalized);
    let Strict(value) = Strict::deserialize(&mut deserializer).map_err(|_| ())?;
    deserializer.end().map_err(|_| ())?;
    if !value.is_object() {
        return Err(());
    }
    Ok(value)
}

// Every numeric position in the pinned closed contract is a nonnegative integer
// bounded by u64::MAX. Normalize mathematical integers before serde can round
// them through f64. Negative i64 integers remain exact for schema rejection.
// This is contract-specific, not a general-purpose JSON number implementation.
fn normalize_numbers(text: &str) -> Result<String, ()> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                let start = i;
                i += 1;
                while i < bytes.len() {
                    match bytes[i] {
                        b'\\' => i = (i + 2).min(bytes.len()),
                        b'"' => {
                            i += 1;
                            break;
                        }
                        _ => i += 1,
                    }
                }
                out.extend_from_slice(&bytes[start..i]);
            }
            b'-' | b'0'..=b'9' => {
                let start = i;
                while i < bytes.len()
                    && matches!(bytes[i], b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
                {
                    i += 1;
                }
                out.extend_from_slice(exact_integer(&bytes[start..i])?.as_bytes());
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| ())
}

fn exact_integer(token: &[u8]) -> Result<String, ()> {
    let mut i = 0;
    let negative = token.first() == Some(&b'-');
    if negative {
        i += 1;
    }
    let start = i;
    match token.get(i) {
        Some(b'0') => i += 1,
        Some(b'1'..=b'9') => {
            while token.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
        }
        _ => return Err(()),
    }
    let mut digits = token[start..i].to_vec();
    let mut fraction_len = 0;
    if token.get(i) == Some(&b'.') {
        i += 1;
        let start = i;
        while token.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        fraction_len = i - start;
        if fraction_len == 0 {
            return Err(());
        }
        digits.extend_from_slice(&token[start..i]);
    }
    let mut exponent = 0i64;
    if matches!(token.get(i), Some(b'e' | b'E')) {
        i += 1;
        let exp_negative = token.get(i) == Some(&b'-');
        if matches!(token.get(i), Some(b'-' | b'+')) {
            i += 1;
        }
        let start = i;
        // Beyond this bound no nonzero token can cancel enough decimal places
        // to fit u64. Saturation bounds work even for adversarial exponents.
        let limit = token.len() as i64 + 21;
        while let Some(digit @ b'0'..=b'9') = token.get(i) {
            exponent = (exponent * 10 + i64::from(digit - b'0')).min(limit);
            i += 1;
        }
        if i == start {
            return Err(());
        }
        if exp_negative {
            exponent = -exponent;
        }
    }
    if i != token.len() {
        return Err(());
    }
    let Some(first) = digits.iter().position(|d| *d != b'0') else {
        return Ok("0".into());
    };
    let last = digits.iter().rposition(|d| *d != b'0').unwrap();
    let trailing = digits.len() - 1 - last;
    let shift = exponent - fraction_len as i64 + trailing as i64;
    if shift < 0 || (last - first + 1) as i64 + shift > 20 {
        return Err(());
    }
    let mut magnitude = 0u64;
    for digit in &digits[first..=last] {
        magnitude = magnitude
            .checked_mul(10)
            .and_then(|n| n.checked_add(u64::from(digit - b'0')))
            .ok_or(())?;
    }
    for _ in 0..shift {
        magnitude = magnitude.checked_mul(10).ok_or(())?;
    }
    if negative {
        if magnitude > (i64::MAX as u64) + 1 {
            return Err(());
        }
        Ok(format!("-{magnitude}"))
    } else {
        Ok(magnitude.to_string())
    }
}
