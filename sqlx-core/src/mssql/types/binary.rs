use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::mssql::io::MssqlBufMutExt;
use crate::mssql::protocol::type_info::{Collation, CollationFlags, DataType, TypeInfo};
use crate::mssql::{Mssql, MssqlTypeInfo, MssqlValueRef};
use crate::types::Type;
use std::vec::Vec;

pub struct Binary{
    inner : Vec<u8>,
}

impl Type<Mssql> for Binary {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo(TypeInfo::new(DataType::VarBinary, 0))
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        matches!(
            ty.0.ty,
            DataType::Binary
                | DataType::BigBinary
                | DataType::VarBinary
                | DataType::BigVarBinary
                | DataType::Image
        )
    }
}

impl Encode<'_, Mssql> for Binary {
    fn produces(&self) -> Option<MssqlTypeInfo> {
        // an empty string needs to be encoded as `nvarchar(2)`
        Some(MssqlTypeInfo(TypeInfo {
            ty: DataType::VarBinary,
            size: self.inner.len() as u32,
            scale: 0,
            precision: 0,
            collation: None,
        }))
    }

    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> IsNull {
        buf.extend_from_slice(self.inner.as_slice());

        IsNull::No
    }
}

impl<'r> Decode<'r, Mssql> for Vec<u8> {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        

        Ok(Vec::new())
    }
}