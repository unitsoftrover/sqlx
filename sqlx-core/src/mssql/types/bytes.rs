use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::mssql::protocol::type_info::{DataType, TypeInfo};
use crate::mssql::{Mssql, MssqlTypeInfo, MssqlValueRef};
use crate::types::Type;

impl Type<Mssql> for [u8] {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo(TypeInfo::new(DataType::BigBinary, 0))
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        matches!(
            ty.0.ty,
            DataType::BigBinary
            | DataType::BigVarBinary
            | DataType::Image
            | DataType::Binary
            | DataType::VarBinary
        )
    }
}

impl Encode<'_, Mssql> for &'_ [u8] {
    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> IsNull {
        buf.extend_from_slice(self);

        IsNull::No
    }
}

impl<'r> Decode<'r, Mssql> for &'r [u8] {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        value.as_bytes()
    }
}

impl Type<Mssql> for Vec<u8> {
    fn type_info() -> MssqlTypeInfo {
        <[u8] as Type<Mssql>>::type_info()
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        <&[u8] as Type<Mssql>>::compatible(ty)
    }
}

impl Encode<'_, Mssql> for Vec<u8> {
    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> IsNull {
        <&[u8] as Encode<Mssql>>::encode(&**self, buf)
    }
}

impl Decode<'_, Mssql> for Vec<u8> {
    fn decode(value: MssqlValueRef<'_>) -> Result<Self, BoxDynError> {
        <&[u8] as Decode<Mssql>>::decode(value).map(ToOwned::to_owned)
    }
}

pub trait ToSqlValue{
    fn to_string(&self)->String;
}

impl ToSqlValue for Vec<u8>{
    fn to_string(&self)->String {
        let vec : Vec<_> = self.iter().map(|item|format!("{:02x}", item)).collect();
        "0x".to_string() + &vec.join("")
    }
}

