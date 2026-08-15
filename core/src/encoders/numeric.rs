//! Postgres' binary `NUMERIC` representation.
//!
//! Everything that ends up on the wire as a `NUMERIC` — the three Arrow decimal widths and
//! `UInt64`, which Postgres has no integer type wide enough for — goes through the functions in
//! this module. The encoders that call them live in [`super::scalar`].

use bytes::BytesMut;

use super::put;

/// Number of base-10000 groups a value of `precision` decimal digits can span once its digits
/// are aligned to the base-10000 group boundaries of the Postgres NUMERIC format.
///
/// A run of `precision` decimal digits covers `ceil(precision / 4)` groups when it happens to be
/// aligned and one more when it straddles a boundary, which is the case for every scale
/// (including negative ones, whose trailing zero groups are never emitted).
#[inline]
pub(super) fn numeric_group_count_hint(precision: u8) -> usize {
    (precision as usize).div_ceil(4) + 1
}

/// Upper bound on the number of base-10000 groups any decimal we can encode occupies.
///
/// The widest backing type is `i128`, whose magnitude has at most 39 decimal digits, and those
/// digits span at most `numeric_group_count_hint(39) == 11` groups.
const MAX_NUMERIC_GROUPS: usize = 11;

macro_rules! encode_decimal {
    ($name:ident, $int:ty, $uint:ty) => {
        /// Encode `value * 10^-scale` in Postgres' binary NUMERIC representation.
        ///
        /// The wire format (see `numeric_send` in `src/backend/utils/adt/numeric.c`) is
        /// `ndigits: i16`, `weight: i16`, `sign: i16`, `dscale: i16` followed by `ndigits`
        /// base-10000 digits, most significant first. The encoded number is
        /// `sum(digits[i] * 10000^(weight - i))`; `sign` is `0x0000` for positive and `0x4000`
        /// for negative; `dscale` is the *display* scale (digits shown after the decimal point).
        ///
        /// Two properties of the format drive the implementation below:
        ///
        /// * the base-10000 groups are aligned on the decimal point, not on the value's digits,
        ///   so the alignment depends only on `scale`;
        /// * `weight` is the base-10000 exponent of the *leading* digit, so it must be derived
        ///   from the position of the digits that are actually emitted (a zero group between the
        ///   decimal point and the first significant fractional digit is part of the value's
        ///   magnitude and must either be emitted or accounted for in `weight`).
        ///
        /// Digits are extracted from the least significant end so that no intermediate value ever
        /// exceeds the backing integer type, and `scale` is never used as an exponent, so
        /// arbitrarily large and negative scales are handled without overflow.
        pub(super) fn $name(value: $int, scale: i8, buf: &mut BytesMut) {
            const NBASE: $uint = 10_000;

            let sign: i16 = if value < 0 { 0x4000 } else { 0 };
            // `unsigned_abs` so that `<$int>::MIN` cannot overflow the negation.
            let mut magnitude: $uint = value.unsigned_abs();

            // The least significant digit of `magnitude` has decimal exponent `-scale`. Shifting
            // the magnitude left by `shift` digits places it on a group boundary, i.e. makes its
            // exponent a multiple of four. `rem_euclid` keeps `shift` in `0..=3` for negative
            // scales too (a negative scale simply means the value has implicit trailing zeros).
            let shift = (-(scale as i32)).rem_euclid(4) as u32;
            // Base-10000 exponent of the least significant group that we are about to emit.
            let low_group_exponent = (-(scale as i32) - shift as i32) / 4;

            // Split the magnitude into base-10000 groups, least significant group first. The
            // lowest group only takes the low `4 - shift` decimal digits, shifted up by `shift`;
            // the multiplication is therefore bounded by `10^4 - 1` and cannot overflow. (The
            // previous implementation multiplied the whole fractional part by `10^shift` up
            // front, which overflowed the backing type at scale >= 9 / 17 / 37.)
            let split = (10 as $uint).pow(4 - shift);
            let mut groups = [0i16; MAX_NUMERIC_GROUPS];
            let mut n_groups = 0usize;
            if magnitude > 0 {
                groups[0] = ((magnitude % split) as u32 * 10_u32.pow(shift)) as i16;
                n_groups = 1;
                magnitude /= split;
                while magnitude > 0 {
                    groups[n_groups] = (magnitude % NBASE) as i16;
                    n_groups += 1;
                    magnitude /= NBASE;
                }
            }

            // The last group written is always non-zero, so there are never leading zero digits.
            // `weight` follows directly from where the groups sit relative to the decimal point,
            // which is what makes interior and leading zero groups come out right.
            let (weight, trailing_zero_groups) = if n_groups == 0 {
                // Zero: Postgres canonicalises this to `ndigits = 0, weight = 0, sign = +`.
                (0, 0)
            } else {
                let weight = low_group_exponent + n_groups as i32 - 1;
                // Postgres stores numerics without trailing zero groups; those live at the front
                // of `groups`, which is ordered least significant first. Dropping them does not
                // change `weight`, which describes the leading digit.
                let trailing = groups[..n_groups].iter().take_while(|d| **d == 0).count();
                (weight, trailing)
            };
            let digits = &groups[trailing_zero_groups..n_groups];

            put(buf, (8 + 2 * digits.len() as i32).to_be_bytes()); // num of bytes
            put(buf, (digits.len() as i16).to_be_bytes());
            put(buf, (weight as i16).to_be_bytes());
            put(buf, (sign).to_be_bytes());
            // `dscale` is the number of digits displayed after the decimal point and cannot be
            // negative on the wire; a negative Arrow scale means the value is an integer.
            put(buf, (scale.max(0) as i16).to_be_bytes());
            // postgres expects the digits to be encoded from largest to smallest, so we
            // need to iterate the slice in reverse
            for d in digits.iter().rev() {
                put(buf, (*d).to_be_bytes());
            }
        }
    };
}

encode_decimal!(encode_decimal_32, i32, u32);
encode_decimal!(encode_decimal_64, i64, u64);
encode_decimal!(encode_decimal_128, i128, u128);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoders::{Decimal64Encoder, Encode};
    use arrow_array::{Decimal128Array, Decimal32Array, Decimal64Array};

    /// The decoded contents of a Postgres binary NUMERIC field.
    #[derive(Debug, PartialEq, Eq)]
    struct Numeric {
        weight: i16,
        sign: i16,
        dscale: i16,
        digits: Vec<i16>,
    }

    impl Numeric {
        /// Render the numeric the way Postgres does (`get_str_from_var`), i.e. the string a
        /// `SELECT` of the loaded value returns. This is what pins the *value* down: a wrong
        /// `weight` or a dropped digit group shows up here even though the bytes are
        /// self-consistent.
        fn to_pg_string(&self) -> String {
            let digit_at = |exponent: i32| -> i16 {
                let idx = self.weight as i32 - exponent;
                if idx < 0 || idx as usize >= self.digits.len() {
                    0
                } else {
                    self.digits[idx as usize]
                }
            };
            let mut out = String::new();
            if self.sign == 0x4000 {
                out.push('-');
            }
            if self.weight < 0 {
                out.push('0');
            } else {
                for exponent in (0..=self.weight as i32).rev() {
                    let group = digit_at(exponent).to_string();
                    if exponent == self.weight as i32 {
                        out.push_str(&group);
                    } else {
                        out.push_str(&format!("{group:0>4}"));
                    }
                }
            }
            if self.dscale > 0 {
                out.push('.');
                let mut fractional = String::new();
                let mut exponent = -1;
                while fractional.len() < self.dscale as usize {
                    fractional.push_str(&format!("{:0>4}", digit_at(exponent)));
                    exponent -= 1;
                }
                fractional.truncate(self.dscale as usize);
                out.push_str(&fractional);
            }
            out
        }
    }

    fn decode(buf: &[u8]) -> Numeric {
        let read_i16 = |at: usize| i16::from_be_bytes([buf[at], buf[at + 1]]);
        let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let ndigits = read_i16(4);
        assert_eq!(len, 8 + 2 * ndigits as i32, "length prefix does not match");
        assert_eq!(buf.len(), 4 + len as usize, "trailing bytes in the buffer");
        let digits: Vec<i16> = (0..ndigits as usize)
            .map(|i| read_i16(12 + 2 * i))
            .collect();
        // Postgres' canonical form: no leading and no trailing zero groups, and every group is a
        // valid base-10000 digit (`numeric_recv` rejects anything else outright).
        assert!(digits.iter().all(|d| (0..10_000).contains(d)), "{digits:?}");
        assert_ne!(digits.first(), Some(&0), "leading zero group: {digits:?}");
        assert_ne!(digits.last(), Some(&0), "trailing zero group: {digits:?}");
        Numeric {
            weight: read_i16(6),
            sign: read_i16(8),
            dscale: read_i16(10),
            digits,
        }
    }

    macro_rules! encode_fn {
        ($name:ident, $int:ty, $encode:ident) => {
            fn $name(value: $int, scale: i8) -> Numeric {
                let mut buf = BytesMut::new();
                $encode(value, scale, &mut buf);
                decode(&buf)
            }
        };
    }
    encode_fn!(enc32, i32, encode_decimal_32);
    encode_fn!(enc64, i64, encode_decimal_64);
    encode_fn!(enc128, i128, encode_decimal_128);

    /// 38 nines, the largest magnitude a `Decimal128(38, _)` can hold.
    const MAX_PRECISION_38: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

    /// The headline corruption from #79: the most significant base-10000 group of the fractional
    /// part is zero, so the old encoder dropped it and shifted every following digit four decimal
    /// places towards the point, silently storing `1.0001`.
    #[test]
    fn leading_zero_fractional_group_is_not_dropped() {
        let numeric = enc64(100_000_001, 8);
        assert_eq!(
            numeric,
            Numeric {
                weight: 0,
                sign: 0,
                dscale: 8,
                digits: vec![1, 0, 1],
            }
        );
        assert_eq!(numeric.to_pg_string(), "1.00000001");
    }

    /// The other two corruptions reported in #79, both pure fractions whose leading group is
    /// zero: the old encoding was off by 10^4 and 10^8 respectively.
    #[test]
    fn pure_fractions_with_leading_zero_groups() {
        let numeric = enc64(6_538_030, 14);
        assert_eq!(
            numeric,
            Numeric {
                weight: -2,
                sign: 0,
                dscale: 14,
                digits: vec![6, 5380, 3000],
            }
        );
        assert_eq!(numeric.to_pg_string(), "0.00000006538030");

        let numeric = enc64(1, 10);
        assert_eq!(
            numeric,
            Numeric {
                weight: -3,
                sign: 0,
                dscale: 10,
                digits: vec![100],
            }
        );
        assert_eq!(numeric.to_pg_string(), "0.0000000001");
    }

    /// Every value of the shape `1 * 10^-scale` puts a run of zero groups between the decimal
    /// point and the single significant digit, which is exactly what used to be mis-weighted.
    #[test]
    fn single_digit_at_every_scale() {
        let expected = |scale: i8| -> String {
            if scale == 0 {
                "1".to_string()
            } else {
                format!("0.{}1", "0".repeat(scale as usize - 1))
            }
        };
        for scale in 0..=9i8 {
            assert_eq!(enc32(1, scale).to_pg_string(), expected(scale), "{scale}");
        }
        for scale in 0..=18i8 {
            assert_eq!(enc64(1, scale).to_pg_string(), expected(scale), "{scale}");
        }
        for scale in 0..=38i8 {
            assert_eq!(enc128(1, scale).to_pg_string(), expected(scale), "{scale}");
        }
    }

    /// Used to panic with "attempt to multiply with overflow" once the padded fractional part no
    /// longer fitted the backing integer (scale >= 9 for i32, >= 17 for i64, >= 37 for i128).
    #[test]
    fn large_scales_do_not_overflow() {
        assert_eq!(enc32(999_999_999, 9).to_pg_string(), "0.999999999");
        assert_eq!(
            enc64(99_999_999_999_999_999, 17).to_pg_string(),
            "0.99999999999999999"
        );
        assert_eq!(enc64(i64::MAX, 18).to_pg_string(), "9.223372036854775807");
        assert_eq!(
            enc128(MAX_PRECISION_38, 37).to_pg_string(),
            "9.9999999999999999999999999999999999999"
        );
        assert_eq!(
            enc128(MAX_PRECISION_38, 38).to_pg_string(),
            "0.99999999999999999999999999999999999999"
        );
        assert_eq!(
            enc128(1, 38).to_pg_string(),
            "0.00000000000000000000000000000000000001"
        );
        // The most negative value of each type has no positive counterpart; negating it used to
        // be the obvious hazard.
        assert_eq!(enc32(i32::MIN, 0).to_pg_string(), i32::MIN.to_string());
        assert_eq!(enc64(i64::MIN, 0).to_pg_string(), i64::MIN.to_string());
        assert_eq!(enc128(i128::MIN, 0).to_pg_string(), i128::MIN.to_string());
    }

    /// Arrow permits negative scales, meaning `value * 10^-scale`. They used to underflow in
    /// `byte_size_hint`; they are now encoded exactly, with `dscale = 0`.
    #[test]
    fn negative_scales() {
        let numeric = enc64(123, -2);
        assert_eq!(
            numeric,
            Numeric {
                weight: 1,
                sign: 0,
                dscale: 0,
                digits: vec![1, 2300],
            }
        );
        assert_eq!(numeric.to_pg_string(), "12300");

        // A shift by a whole number of base-10000 groups: the trailing zero groups are not
        // emitted at all, they only move `weight`.
        let numeric = enc32(1, -8);
        assert_eq!(
            numeric,
            Numeric {
                weight: 2,
                sign: 0,
                dscale: 0,
                digits: vec![1],
            }
        );
        assert_eq!(numeric.to_pg_string(), "100000000");

        assert_eq!(enc32(-7, -1).to_pg_string(), "-70");
        assert_eq!(enc32(999_999_999, -9).to_pg_string(), "999999999000000000");
        assert_eq!(
            enc128(MAX_PRECISION_38, -38).to_pg_string(),
            format!("{MAX_PRECISION_38}{}", "0".repeat(38))
        );
    }

    #[test]
    fn zero_is_canonical() {
        for scale in [-4i8, -1, 0, 1, 6, 38] {
            assert_eq!(
                enc128(0, scale),
                Numeric {
                    weight: 0,
                    sign: 0,
                    dscale: scale.max(0) as i16,
                    digits: vec![],
                },
                "scale {scale}"
            );
        }
        assert_eq!(enc32(0, 3).to_pg_string(), "0.000");
    }

    #[test]
    fn signs_and_trailing_zeros() {
        assert_eq!(enc32(-123_450_000, 6).to_pg_string(), "-123.450000");
        assert_eq!(enc32(123_450_000, 6).to_pg_string(), "123.450000");
        assert_eq!(enc32(123_000_000, 6).to_pg_string(), "123.000000");
        assert_eq!(enc32(1_000, 6).to_pg_string(), "0.001000");
        // The `UInt64` conversion encodes through the 128 bit path with scale 0.
        assert_eq!(
            enc128(u64::MAX as i128, 0).to_pg_string(),
            u64::MAX.to_string()
        );
    }

    /// Arrow renders a zero with a negative scale by appending the zeros literally (`0` at scale
    /// `-9` becomes `"0000000000"`); Postgres renders the same value as `"0"`. Normalise the
    /// integer part so the two can be compared.
    fn strip_leading_zeros(rendered: &str) -> String {
        let (sign, rest) = match rendered.strip_prefix('-') {
            Some(rest) => ("-", rest),
            None => ("", rendered),
        };
        let (integer, fractional) = match rest.split_once('.') {
            Some((integer, fractional)) => (integer, Some(fractional)),
            None => (rest, None),
        };
        let trimmed = integer.trim_start_matches('0');
        let integer = if trimmed.is_empty() { "0" } else { trimmed };
        let sign = if integer == "0" && fractional.is_none_or(|f| f.bytes().all(|b| b == b'0')) {
            ""
        } else {
            sign
        };
        match fractional {
            Some(fractional) => format!("{sign}{integer}.{fractional}"),
            None => format!("{sign}{integer}"),
        }
    }

    /// Cross-check a sweep of values against Arrow's own rendering of the same decimal, for every
    /// scale (negative ones included) that Arrow accepts for the type.
    macro_rules! cross_check_with_arrow {
        ($name:ident, $arr:ty, $int:ty, $encode:ident, $precision:expr) => {
            #[test]
            fn $name() {
                // The extremes are the largest magnitude the precision allows; Arrow's own
                // renderer misbehaves past that, and Postgres would reject the column anyway.
                let extreme = (10 as $int).pow($precision as u32 - 1) * 9
                    + (10 as $int).pow($precision as u32 - 1)
                    - 1;
                let values: Vec<$int> = vec![
                    0,
                    1,
                    -1,
                    7,
                    10,
                    9_999,
                    10_000,
                    10_001,
                    100_000_001,
                    123_456_789,
                    extreme,
                    -extreme,
                ];
                for scale in -$precision..=$precision {
                    let arr = <$arr>::from(values.clone())
                        .with_precision_and_scale($precision as u8, scale)
                        .unwrap();
                    for row in 0..arr.len() {
                        let mut buf = BytesMut::new();
                        $encode(arr.value(row), scale, &mut buf);
                        assert_eq!(
                            decode(&buf).to_pg_string(),
                            strip_leading_zeros(&arr.value_as_string(row)),
                            "value {} at scale {scale}",
                            arr.value(row)
                        );
                    }
                }
            }
        };
    }
    cross_check_with_arrow!(
        cross_check_decimal32,
        Decimal32Array,
        i32,
        encode_decimal_32,
        9i8
    );
    cross_check_with_arrow!(
        cross_check_decimal64,
        Decimal64Array,
        i64,
        encode_decimal_64,
        18i8
    );
    cross_check_with_arrow!(
        cross_check_decimal128,
        Decimal128Array,
        i128,
        encode_decimal_128,
        38i8
    );

    /// `byte_size_hint` used to compute `precision - scale` in `usize`, which underflowed (and
    /// panicked) for the negative scales Arrow allows. Encoding a whole array of them has to work
    /// end to end too.
    #[test]
    fn byte_size_hint_handles_negative_scales() {
        for (precision, scale) in [
            (9u8, -9i8),
            (9, -1),
            (9, 0),
            (9, 4),
            (9, 9),
            (1, -1),
            (1, 1),
        ] {
            let max = 10_i64.pow(precision as u32) - 1;
            let arr = Decimal64Array::from(vec![max, -max, 0, 1])
                .with_precision_and_scale(precision, scale)
                .unwrap();
            let encoder = Decimal64Encoder::new(&arr);
            let hint = encoder.byte_size_hint().unwrap();
            assert_eq!(
                hint,
                // 4 byte field length + 8 byte NUMERIC header + the digit groups.
                arr.len() * (12 + 2 * numeric_group_count_hint(precision)),
                "({precision}, {scale})"
            );
            let mut buf = BytesMut::new();
            for row in 0..arr.len() {
                encoder.encode(row, &mut buf).unwrap();
            }
            // The hint has to cover what was written, or `write_batch` under-reserves.
            assert!(
                buf.len() <= hint,
                "wrote {} bytes against a hint of {hint} for ({precision}, {scale})",
                buf.len()
            );
            // Every digit group the values actually needed fits in the hinted group count.
            let mut rest = &buf[..];
            while !rest.is_empty() {
                let len = 4 + i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
                let numeric = decode(&rest[..len]);
                assert!(
                    numeric.digits.len() <= numeric_group_count_hint(precision),
                    "{numeric:?} exceeds the hinted group count for ({precision}, {scale})"
                );
                rest = &rest[len..];
            }
        }
    }
}
