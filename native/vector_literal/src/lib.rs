//! Experimental in-process ABI: a *single* 1024-f64 vector -> pgvector text.
//! No model, SQL, credentials, scheduler or production integration is included.
//! The Python caller owns exact list/type checks and conversion, which the
//! benchmark includes in C wall/CPU times.
//!
//! Returned integer statuses:
//! 0 OK, 1 dimension, 2 non-finite, 3 L2 norm, 4 buffer too small,
//! 5 invalid pointer. Nothing is written unless status is zero.

use std::fmt::Write as _;
use std::ptr;

const DIM: usize = 1024;
const NORM_TOLERANCE: f64 = 0.002;

/// Normalize the shortest round-trippable Rust f64 debug form into Python's
/// str(float) exponent window [-4, 15], decimal point and exponent padding.
/// Exact text parity is tested; no tolerance or result substitution is allowed.
fn python_float(value: f64) -> String {
    if value == 0.0 {
        return if value.is_sign_negative() { "-0.0" } else { "0.0" }.into();
    }
    // Rust's shortest Debug choice may round decimal half-ways upward,
    // while CPython str(float) chooses the even last decimal digit.
    // Request the SAME shortest significant precision explicitly, so the
    // formatter rounds ties-to-even before converting its layout to Python.
    let shortest = format!("{value:?}");
    let unsigned = shortest.trim_start_matches('-');
    let (mantissa0, e0) = match unsigned.split_once('e') {
        Some((m, e)) => (m, e.parse::<i32>().expect("float exponent")),
        None => (unsigned, 0),
    };
    let point0 = mantissa0.find('.').unwrap_or(mantissa0.len()) as i32;
    let digits0: String = mantissa0.chars().filter(|c| c.is_ascii_digit()).collect();
    let leading0 = digits0.bytes().take_while(|b| *b == b'0').count() as i32;
    let shortest_digits = digits0[leading0 as usize..].trim_end_matches('0').len();
    let exponent0 = e0 + point0 - 1 - leading0;
    let s = if (-4..=15).contains(&exponent0) {
        let places = (shortest_digits as i32 - exponent0 - 1).max(0) as usize;
        format!("{:.*}", places, value)
    } else {
        format!("{:.*e}", shortest_digits.saturating_sub(1), value)
    };
    let (sign, core) = if let Some(tail) = s.strip_prefix('-') {
        ("-", tail)
    } else {
        ("", s.as_str())
    };
    let (mantissa, exp) = match core.split_once('e') {
        Some((m, e)) => (m, e.parse::<i32>().expect("Rust finite float exponent")),
        None => (core, 0),
    };
    let point = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let digits: String = mantissa.chars().filter(|c| c.is_ascii_digit()).collect();
    let zeros = digits.bytes().take_while(|b| *b == b'0').count() as i32;
    let significant = &digits[zeros as usize..];
    // Already handled zero above. Remove redundant trailing zeros without
    // changing the scientific exponent.
    let short = significant.trim_end_matches('0');
    let exponent = exp + point - 1 - zeros;
    let sig = if short.is_empty() { "0" } else { short };
    let mut out = String::with_capacity(32);
    out.push_str(sign);
    if (-4..=15).contains(&exponent) {
        let decimal = exponent + 1;
        if decimal <= 0 {
            out.push_str("0.");
            for _ in 0..-decimal { out.push('0'); }
            out.push_str(sig);
        } else {
            let split = decimal as usize;
            if sig.len() > split {
                out.push_str(&sig[..split]);
                out.push('.');
                out.push_str(&sig[split..]);
            } else {
                out.push_str(sig);
                for _ in 0..split.saturating_sub(sig.len()) { out.push('0'); }
                out.push_str(".0");
            }
        }
    } else {
        out.push(sig.as_bytes()[0] as char);
        if sig.len() > 1 {
            out.push('.');
            out.push_str(&sig[1..]);
        }
        let _ = write!(&mut out, "e{}{abs:02}", if exponent >= 0 { '+' } else { '-' },
            abs = exponent.abs());
    }
    out
}

/// # Safety
/// The caller must supply a valid contiguous len-element f64 input allocation,
/// a writable capacity-byte output allocation and a writable written pointer.
/// Only the local trusted Python ctypes wrapper calls this ABI. No external input
/// is authorized by this function.
#[no_mangle]
pub unsafe extern "C" fn rkb_vector_literal_v1(
    values: *const f64,
    len: usize,
    output: *mut u8,
    capacity: usize,
    written: *mut usize,
) -> i32 {
    if len != DIM { return 1; }
    if values.is_null() || output.is_null() || written.is_null() { return 5; }
    let input = unsafe { std::slice::from_raw_parts(values, len) };
    if input.iter().any(|v| !v.is_finite()) { return 2; }
    let mut squares = 0.0f64;
    for &value in input.iter() {
        squares += value * value;
    }
    if (squares.sqrt() - 1.0).abs() > NORM_TOLERANCE { return 3; }

    let mut text = String::with_capacity(24576);
    text.push('[');
    for (i, &value) in input.iter().enumerate() {
        if i != 0 { text.push(','); }
        text.push_str(&python_float(value));
    }
    text.push(']');
    let bytes = text.as_bytes();
    if bytes.len() > capacity { return 4; }
    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr(), output, bytes.len());
        *written = bytes.len();
    }
    0
}

#[cfg(test)]
mod tests {
    use super::python_float;
    #[test]
    fn python_number_window() {
        for (input, expected) in [
            (1.0, "1.0"),
            (-0.0, "-0.0"),
            (1e-4, "0.0001"),
            (1e-5, "1e-05"),
            (1e15, "1000000000000000.0"),
            (1e16, "1e+16"),
            (-1e-10, "-1e-10"),
        ] {
            assert_eq!(python_float(input), expected);
        }
    }
}
