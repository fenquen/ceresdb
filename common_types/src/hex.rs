// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

/// Try to decode bytes from hex literal string.
///
/// None will be returned if the input literal is hex-invalid.
pub fn try_decode(s: &str) -> Option<Vec<u8>> {
    let hex_bytes = s.as_bytes();

    let mut decoded_bytes = Vec::with_capacity((hex_bytes.len() + 1) / 2);

    let start_idx = hex_bytes.len() % 2;
    if start_idx > 0 {
        // The first byte is formed of only one char.
        decoded_bytes.push(try_decode_hex_char(hex_bytes[0])?);
    }

    for i in (start_idx..hex_bytes.len()).step_by(2) {
        let high = try_decode_hex_char(hex_bytes[i])?;
        let low = try_decode_hex_char(hex_bytes[i + 1])?;
        decoded_bytes.push(high << 4 | low);
    }

    Some(decoded_bytes)
}

/// Try to decode a byte from a hex char.
///
/// None will be returned if the input char is hex-invalid.
const fn try_decode_hex_char(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'F' => Some(c - b'A' + 10),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'0'..=b'9' => Some(c - b'0'),
        _ => None,
    }
}