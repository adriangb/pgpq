use postgres_types::{to_sql_checked, IsNull, ToSql, Type};

use crate::error::Error;

use bytes::{BufMut, BytesMut};

pub const HEADER_MAGIC_BYTES: &[u8] = b"PGCOPY\n\xff\r\n\0";

#[inline]
pub(crate) fn write_null(buf: &mut BytesMut) {
    buf.put_i32(-1);
}

#[inline]
pub(crate) fn write_value(
    v: &impl ToSql,
    type_: &Type,
    field_name: &str,
    buf: &mut BytesMut,
) -> Result<(), Error> {
    let idx = buf.len();
    buf.put_i32(0); // save space for field length word
    let len = match v
        .to_sql_checked(type_, buf)
        .map_err(|e| Error::to_sql(e, field_name))?
    {
        IsNull::Yes => -1,
        IsNull::No => {
            let written = buf.len() - idx - 4; // 4 comes from the i32 we put above
            i32::try_from(written).map_err(|_| Error::field_too_large(field_name, written))?
        }
    };
    buf[idx..idx+4].copy_from_slice(&len.to_be_bytes());
    Ok(())
}

#[derive(Debug)]
pub(crate) struct PostgresDuration {
    pub duration: chrono::Duration,
}

#[derive(Debug)]
struct OverflowError {
    value: chrono::Duration,
}

impl std::fmt::Display for OverflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Overflow converting duration to microseconds. Value: {}",
            self.value
        )
    }
}

impl std::error::Error for OverflowError {}

impl ToSql for PostgresDuration {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<(dyn std::error::Error + Send + Sync)>> {
        let micros = match self.duration.num_microseconds() {
            Some(v) => v,
            None => {
                return Err(Box::new(OverflowError {
                    value: self.duration,
                }))
            }
        };
        out.put_i64(micros);
        Ok(IsNull::No)
    }
    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::INTERVAL)
    }

    to_sql_checked!();
}
