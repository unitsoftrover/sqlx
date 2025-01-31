// use std::mem;

use byteorder::{ByteOrder, LittleEndian};
use chrono::{DateTime, Duration, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc, Offset, FixedOffset};

use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::mssql::protocol::type_info::{DataType, TypeInfo};
use crate::mssql::{Mssql, MssqlTypeInfo, MssqlValueRef};
use crate::types::Type;

impl Type<Mssql> for NaiveTime {
    fn type_info() -> MssqlTypeInfo {
        let mut ty = TypeInfo::new(DataType::TimeN, 5);
        ty.scale = 7;
        MssqlTypeInfo(ty)
    }
}

impl Type<Mssql> for NaiveDate {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo(TypeInfo::new(DataType::DateN, 3))
    }
}

impl Type<Mssql> for NaiveDateTime {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo(TypeInfo::new(DataType::DateTimeN, 8))
    }

    fn compatible(ty: &<Mssql as crate::database::Database>::TypeInfo) -> bool {
        matches!(
            ty.0.ty,
            DataType::DateTimeN
            | DataType::DateTime
        )
    }
}

impl<Tz: TimeZone> Type<Mssql> for DateTime<Tz> {
    fn type_info() -> MssqlTypeInfo {
        let mut ty = TypeInfo::new(DataType::DateTimeOffsetN, 10);
        ty.scale = 7;
        MssqlTypeInfo(ty)
    }

    fn compatible(ty: &<Mssql as crate::database::Database>::TypeInfo) -> bool {
        matches!(
            ty.0.ty,
             DataType::DateTime2N
            |DataType::DateTimeOffsetN
        )
    }
}

// impl Type<Mssql> for DateTime<Local> {
//     fn type_info() -> MssqlTypeInfo {
//         let mut ty = TypeInfo::new(DataType::DateTimeOffsetN, 10);
//         ty.scale = 7;
//         MssqlTypeInfo(ty)
//     }
// }

// impl Type<Mssql> for DateTime<Utc> {
//     fn type_info() -> MssqlTypeInfo {
//         let mut ty = TypeInfo::new(DataType::DateTimeOffsetN, 10);
//         ty.scale = 7;
//         MssqlTypeInfo(ty)
//     }
// }

impl Encode<'_, Mssql> for NaiveTime {
    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> IsNull {
        // let ms_duration = *self - NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        // let ms = ms_duration.num_milliseconds() as u32 * 3 / 10;
        // buf.extend_from_slice(&ms.to_le_bytes());

        // time(n)精度与长度的关系
        //0..2=>3
        //3..4=>4
        //5..7=>5
        let ms_duration = *self - NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        let ms = ms_duration.num_seconds() as u64 * 10_u64.pow(7);
        //time
        buf.extend(&ms.to_le_bytes()[0..5]);

        IsNull::No
    }
}

impl<'r> Decode<'r, Mssql> for NaiveTime {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        // let third_seconds = LittleEndian::read_u32(&value.as_bytes()?[0..4]);
        // let ms = third_seconds / 3 * 10;
        // let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap_or_default() + Duration::milliseconds(ms.into());
        let mut time = LittleEndian::read_u32(&value.as_bytes()?[0..4]) as u64;
        time |= (value.as_bytes()?[4] as u64) << 32;
        time = time / 10_u64.pow(4);
        let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap() + Duration::milliseconds(time as i64);
        
        Ok(time)
    }
}

impl Encode<'_, Mssql> for NaiveDate {
    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> IsNull {
        let date = self.and_hms_opt(0, 0, 0).unwrap();        
        let days_duration = date.date() - NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
        let days = days_duration.num_days() as i32;
        buf.extend(&days.to_le_bytes());

        IsNull::No
    }
}

impl<'r> Decode<'r, Mssql> for NaiveDate {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        let mut days = LittleEndian::read_u16(&value.as_bytes()?[0..2]) as u32;
        days |= (value.as_bytes()?[2] as u32) << 16;

        let date = NaiveDate::from_ymd_opt(1, 1, 1).unwrap() + Duration::days(days.into());
        Ok(date)
    }
}

impl Encode<'_, Mssql> for NaiveDateTime {
    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> IsNull {
        let days_duration = self.date() - NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
        let ms_duration = self.time() - NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        let days = days_duration.num_days() as i32;
        let ms = ms_duration.num_milliseconds() as u32 * 3 / 10;

        buf.extend(&days.to_le_bytes());
        buf.extend_from_slice(&ms.to_le_bytes());

        IsNull::No
    }
}

impl<'r> Decode<'r, Mssql> for NaiveDateTime {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        let days = LittleEndian::read_i32(&value.as_bytes()?[0..4]);
        let third_seconds = LittleEndian::read_u32(&value.as_bytes()?[4..8]);
        let ms = third_seconds / 3 * 10;

        let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap() + Duration::milliseconds(ms.into());
        let date = NaiveDate::from_ymd_opt(1900, 1, 1).unwrap() + Duration::days(days.into());

        Ok(date.and_time(time))
    }
}


impl<Tz: TimeZone> Encode<'_, Mssql> for DateTime<Tz> {
    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> IsNull {

        //datetimeoffset(7) len:1 time:5 date:3 offset:2 total:11
        // time(n)精度与长度的关系
        //0..2=>3
        //3..4=>4
        //5..7=>5
        let datetime = self.naive_utc();
        let days_duration = datetime.date() - NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
        let ms_duration = datetime.time() - NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        let days = days_duration.num_days() as i32;
        let ms = ms_duration.num_seconds() as u64 * 10_u64.pow(7);
        //time
        buf.extend(&ms.to_le_bytes()[0..5]);
        //date
        buf.extend(&days.to_le_bytes()[0..3]);
        //offset
        let offset = (self.offset().fix().local_minus_utc() / 60) as i16;
        buf.extend_from_slice(&offset.to_le_bytes());
        
        IsNull::No
    }
}

impl<'r> Decode<'r, Mssql> for DateTime<Local> {
    fn decode(value: MssqlValueRef<'_>) -> Result<Self, BoxDynError> {
        let mut time = LittleEndian::read_u32(&value.as_bytes()?[0..4]) as u64;
        time |= (value.as_bytes()?[4] as u64) << 32;
        time = time / 10_u64.pow(4);

        let mut days = LittleEndian::read_u16(&value.as_bytes()?[5..7]) as u32;
        days |= (value.as_bytes()?[7] as u32) << 16;


        let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap() + Duration::milliseconds(time as i64);
        let date = NaiveDate::from_ymd_opt(1, 1, 1).unwrap() + Duration::days(days.into());

        if value.type_info.0.ty == DataType::DateTimeOffsetN{
            let secs = LittleEndian::read_u16(&value.as_bytes()?[8..10]) as u32 * 60;
            let tz = FixedOffset::east_opt(secs as i32).unwrap();           
            Ok(DateTime::<Local>::from_naive_utc_and_offset(date.and_time(time), tz))
        }
        else{
            Ok(Local::from_local_datetime(&Local, &date.and_time(time)).unwrap())
        }
    }
}


impl<'r> Decode<'r, Mssql> for DateTime<Utc> {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        let mut time = LittleEndian::read_u32(&value.as_bytes()?[0..4]) as u64;
        time |= (value.as_bytes()?[4] as u64) << 32;
        time = time / 10_u64.pow(4);

        let mut days = LittleEndian::read_u16(&value.as_bytes()?[5..7]) as u32;
        days |= (value.as_bytes()?[7] as u32) << 16;


        let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap() + Duration::milliseconds(time as i64);
        let date = NaiveDate::from_ymd_opt(1, 1, 1).unwrap() + Duration::days(days.into());

        if value.type_info.0.ty == DataType::DateTimeOffsetN{
            let secs = LittleEndian::read_u16(&value.as_bytes()?[8..10]) as u32 * 60;
            let tz = FixedOffset::east_opt(secs as i32).unwrap();
            let local = DateTime::<Local>::from_naive_utc_and_offset(date.and_time(time), tz);        
            Ok(DateTime::<Utc>::from_naive_utc_and_offset(local.naive_utc(), Utc))        
        }
        else{            
            Ok(Utc::from_local_datetime(&Utc, &date.and_time(time)).unwrap())
        }
    }
}

impl<'r> Decode<'r, Mssql> for DateTime<FixedOffset> {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        let mut time = LittleEndian::read_u32(&value.as_bytes()?[0..4]) as u64;
        time |= (value.as_bytes()?[4] as u64) << 32;
        time = time / 10_u64.pow(4);

        let mut days = LittleEndian::read_u16(&value.as_bytes()?[5..7]) as u32;
        days |= (value.as_bytes()?[7] as u32) << 16;


        let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap() + Duration::milliseconds(time as i64);
        let date = NaiveDate::from_ymd_opt(1, 1, 1).unwrap() + Duration::days(days.into());

        let secs = LittleEndian::read_u16(&value.as_bytes()?[8..10]) as u32 * 60;
        let tz = FixedOffset::east_opt(secs as i32).unwrap();
        Ok(DateTime::<FixedOffset>::from_naive_utc_and_offset(date.and_time(time), tz))
    }
}
