use crate::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,    
    mssql::{MssqlTypeInfo, MssqlValueRef, Mssql},
    types::Type, 
};
use crate::mssql::protocol::type_info::{DataType, TypeInfo};
use bit_vec::BitVec;
use std:: mem;



impl Type<Mssql> for BitVec {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo(TypeInfo::new(DataType::VarBinary, 0))
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        matches!(ty.0.ty, DataType::BigBinary | DataType::BigVarBinary | DataType::Binary | DataType::Image)
    }
}


impl Encode<'_, Mssql> for BitVec {
    fn produces(&self) -> Option<MssqlTypeInfo> {
        // an empty string needs to be encoded as `nvarchar(2)`
        Some(MssqlTypeInfo(TypeInfo {
            ty: DataType::VarBinary,
            size: self.len() as u32,
            scale: 0,
            precision: 0,
            collation: None,
        }))
    }

    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> IsNull {
        buf.extend(&(self.len() as i32).to_be_bytes());
        buf.extend(self.to_bytes());

        IsNull::No
    }

    fn size_hint(&self) -> usize {
        mem::size_of::<i32>() + self.len()
    }
}

impl Decode<'_, Mssql> for BitVec {
    fn decode(value: MssqlValueRef<'_>) -> Result<Self, BoxDynError> {
        let mut bit_vec = BitVec::default();            
        if let Some(data) = value.data{         
            let mut vec = BitVec::from_bytes(&data);
            bit_vec.append(&mut vec);                   
        }
        Ok(bit_vec)
    }
}

