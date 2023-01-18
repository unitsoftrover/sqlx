use bigdecimal::BigDecimal;
use bigdecimal_::num_traits::Pow;
use bigdecimal_::ToPrimitive;
use bytes::BufMut;

use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::mssql::protocol::type_info::{DataType, TypeInfo};
use crate::mssql::{Mssql, MssqlTypeInfo, MssqlValueRef};
use crate::types::Type;

impl Type<Mssql> for BigDecimal {
    fn type_info() -> MssqlTypeInfo {
        // MssqlTypeInfo(TypeInfo::new(DataType::DecimalN, 9))
        MssqlTypeInfo(TypeInfo {
            ty: DataType::DecimalN,
            size: 17,
            precision: 38,
            scale: 8,
            collation: None,
        })
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        matches!(
            ty.0.ty,
            DataType::Decimal
                | DataType::Numeric
                | DataType::DecimalN
                | DataType::NumericN
                | DataType::Money
                | DataType::MoneyN
        )
    }
}

impl Encode<'_, Mssql> for BigDecimal {
    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> IsNull {
        let size = Self::type_info().0.size;
        let scale = Self::type_info().0.scale;

        match self.sign() {
            num_bigint::Sign::Plus => {
                buf.push(1);
            }
            num_bigint::Sign::NoSign => {
                buf.push(1);
            }
            num_bigint::Sign::Minus => {
                buf.push(0);
            }
        }
        let abs = self.abs();
        let value: u128;
        let (bigint, exp) = abs.as_bigint_and_exponent();
        if exp < scale as i64 {
            value = bigint.to_u128().unwrap_or_default()
                * 10u64.pow((scale as i64 - exp) as u32) as u128;
        } else {
            value = bigint.to_u128().unwrap_or_default()
                / 10u64.pow((exp - scale as i64) as u32) as u128;
        }

        match size {
            5 => {
                buf.put_u32_le(value as u32);
            }
            9 => {
                buf.put_u64_le(value as u64);
            }
            13 => {
                buf.put_u64_le(value as u64);
                buf.put_u32_le((value >> 64) as u32);
            }
            17 => {
                buf.put_u128_le(value);
            }
            _x => {}
        }

        IsNull::No
    }
}

impl Decode<'_, Mssql> for BigDecimal {
    fn decode(value: MssqlValueRef<'_>) -> Result<Self, BoxDynError> {
        let mut bytes = value.as_bytes()?;
        let mut sign: i128 = 1;

        let scale: u8;
        if value.type_info.0.ty != DataType::Money {
            sign = bytes[0] as i128;
            if sign == 0 {
                sign = -1;
            }
            bytes = &bytes[1..bytes.len()];
            scale = value.type_info.0.scale;
        } else {
            let mut use_low4_bytes = false;
            for i in 0..4 {
                if bytes[i] != 0 {
                    use_low4_bytes = true;
                    break;
                }
            }
            if !use_low4_bytes {
                bytes = &bytes[4..bytes.len()];
            }
            scale = 4;
        }
        let n = bytes.len();

        let mut decimal_value = 0i128;
        let mut base = 1i128;
        for i in 0..n {
            let current = bytes[i];
            let low_half = current % 16;
            let high_half = current / 16;
            decimal_value += base * low_half as i128;
            base *= 16i128;
            decimal_value += base * high_half as i128;
            base *= 16i128;
        }
        println!("sign:{} decimal value:{}", sign, decimal_value);
        let decimal = bigdecimal::BigDecimal::new(
            num_bigint::BigInt::from(sign * decimal_value),
            scale.into(),
        );
        println!("decimal result:{}", decimal.to_string());
        return Ok(decimal);
    }
}
