mod error;

use std::ops::Not;

use crate::error::Error;
use crate::postgres::{write_null, write_value, PostgresDuration, HEADER_MAGIC_BYTES};
use crate::arrays::{downcast_array, ArrowArray};

use arrow_schema::Schema;
use arrow_array::RecordBatch;
use arrow_array::{Array, ArrowPrimitiveType, PrimitiveArray};
use arrow_schema::Field as ArrowField;
use bytes::{BufMut, BytesMut};
use chrono::{TimeZone, Utc, DateTime, NaiveDateTime, NaiveDate, NaiveTime};
use postgres_types::Type as PostgresType;

mod arrays;
mod postgres;

#[inline]
fn check_null_mask<T>(arr: T, index: usize) -> Option<T>
where
    T: Array,
{
    if arr.is_null(index) {
        None
    } else {
        Some(arr)
    }
}

#[derive(Debug, PartialEq)]
enum EncoderState {
    Created,
    Encoding,
    Finished,
}

#[derive(Debug)]
pub struct ArrowToPostgresBinaryEncoder {
    fields: Vec<ArrowField>,
    state: EncoderState,
}

#[inline]
fn value_from_primitive_array<T: ArrowPrimitiveType>(
    arr: &PrimitiveArray<T>,
    row: usize,
) -> Option<T::Native> {
    arr.is_null(row).not().then(|| arr.value(row))
}

#[inline]
fn write_array(arr: ArrowArray, field_name: &str, buf: &mut BytesMut) -> Result<(), Error> {
    match arr {
        ArrowArray::Boolean(arr) => {
            let v: Vec<Option<bool>> = arr.iter().collect();
            write_value(&v, &PostgresType::BOOL_ARRAY, field_name, buf)
        }
        ArrowArray::Int8(arr) => {
            let v: Vec<Option<i16>> = arr.iter().map(|v| v.map(i16::from)).collect();
            write_value(&v, &PostgresType::INT2_ARRAY, field_name, buf)
        }
        ArrowArray::Int16(arr) => {
            let v: Vec<Option<i16>> = arr.iter().collect();
            write_value(&v, &PostgresType::INT2_ARRAY, field_name, buf)
        }
        ArrowArray::Int32(arr) => {
            let v: Vec<Option<i32>> = arr.iter().collect();
            write_value(&v, &PostgresType::INT4_ARRAY, field_name, buf)
        }
        ArrowArray::Int64(arr) => {
            let v: Vec<Option<i64>> = arr.iter().collect();
            write_value(&v, &PostgresType::INT8_ARRAY, field_name, buf)
        }
        ArrowArray::UInt8(arr) => {
            let v: Vec<Option<i16>> = arr.iter().map(|v| v.map(i16::from)).collect();
            write_value(&v, &PostgresType::INT2_ARRAY, field_name, buf)
        }
        ArrowArray::UInt16(arr) => {
            let v: Vec<Option<i32>> = arr.iter().map(|v| v.map(i32::from)).collect();
            write_value(&v, &PostgresType::INT4_ARRAY, field_name, buf)
        }
        ArrowArray::UInt32(arr) => {
            let v: Vec<Option<i64>> = arr.iter().map(|v| v.map(i64::from)).collect();
            write_value(&v, &PostgresType::INT8_ARRAY, field_name, buf)
        }
        ArrowArray::Float16(arr) => {
            let v: Vec<Option<f32>> = arr.iter().map(|v| v.map(f32::from)).collect();
            write_value(&v, &PostgresType::FLOAT4_ARRAY, field_name, buf)
        }
        ArrowArray::Float32(arr) => {
            let v: Vec<Option<f32>> = arr.iter().collect();
            write_value(&v, &PostgresType::FLOAT4_ARRAY, field_name, buf)
        }
        ArrowArray::Float64(arr) => {
            let v: Vec<Option<f64>> = arr.iter().collect();
            write_value(&v, &PostgresType::FLOAT8_ARRAY, field_name, buf)
        }
        ArrowArray::TimestampNanosecond(arr, true) => {
            let v: Vec<Option<DateTime<Utc>>> = (0..arr.len()).map(|idx|arr.value_as_datetime(idx).map(|v|Utc.from_utc_datetime(&v))).collect();
            write_value(&v, &PostgresType::TIMESTAMPTZ_ARRAY, field_name, buf)
        }
        ArrowArray::TimestampMicrosecond(arr, true) => {
            let v: Vec<Option<DateTime<Utc>>> = (0..arr.len()).map(|idx|arr.value_as_datetime(idx).map(|v|Utc.from_utc_datetime(&v))).collect();
            write_value(&v, &PostgresType::TIMESTAMPTZ_ARRAY, field_name, buf)
        }
        ArrowArray::TimestampMillisecond(arr, true) => {
            let v: Vec<Option<DateTime<Utc>>> = (0..arr.len()).map(|idx|arr.value_as_datetime(idx).map(|v|Utc.from_utc_datetime(&v))).collect();
            write_value(&v, &PostgresType::TIMESTAMPTZ_ARRAY, field_name, buf)
        }
        ArrowArray::TimestampSecond(arr, true) => {
            let v: Vec<Option<DateTime<Utc>>> = (0..arr.len()).map(|idx|arr.value_as_datetime(idx).map(|v|Utc.from_utc_datetime(&v))).collect();
            write_value(&v, &PostgresType::TIMESTAMPTZ_ARRAY, field_name, buf)
        }
        ArrowArray::TimestampNanosecond(arr, false) => {
            let v: Vec<Option<NaiveDateTime>> = (0..arr.len()).map(|idx|arr.value_as_datetime(idx)).collect();
            write_value(&v, &PostgresType::TIMESTAMP_ARRAY, field_name, buf)
        }
        ArrowArray::TimestampMicrosecond(arr, false) => {
            let v: Vec<Option<NaiveDateTime>> = (0..arr.len()).map(|idx|arr.value_as_datetime(idx)).collect();
            write_value(&v, &PostgresType::TIMESTAMP_ARRAY, field_name, buf)
        }
        ArrowArray::TimestampMillisecond(arr, false) => {
            let v: Vec<Option<NaiveDateTime>> = (0..arr.len()).map(|idx|arr.value_as_datetime(idx)).collect();
            write_value(&v, &PostgresType::TIMESTAMP_ARRAY, field_name, buf)
        }
        ArrowArray::TimestampSecond(arr, false) => {
            let v: Vec<Option<NaiveDateTime>> = (0..arr.len()).map(|idx|arr.value_as_datetime(idx)).collect();
            write_value(&v, &PostgresType::TIMESTAMP_ARRAY, field_name, buf)
        }
        ArrowArray::Date32(arr) => {
            let v: Vec<Option<NaiveDate>> = (0..arr.len()).map(|idx|arr.value_as_date(idx)).collect();
            write_value(&v, &PostgresType::DATE_ARRAY, field_name, buf)
        }
        ArrowArray::Date64(arr) => {
            let v: Vec<Option<NaiveDate>> = (0..arr.len()).map(|idx|arr.value_as_date(idx)).collect();
            write_value(&v, &PostgresType::DATE_ARRAY, field_name, buf)
        }
        ArrowArray::Time32Millisecond(arr) => {
            let v: Vec<Option<NaiveTime>> = (0..arr.len()).map(|idx|arr.value_as_time(idx)).collect();
            write_value(&v, &PostgresType::TIME_ARRAY, field_name, buf)
        }
        ArrowArray::Time32Second(arr) => {
            let v: Vec<Option<NaiveTime>> = (0..arr.len()).map(|idx|arr.value_as_time(idx)).collect();
            write_value(&v, &PostgresType::TIME_ARRAY, field_name, buf)
        }
        ArrowArray::Time64Microsecond(arr) => {
            let v: Vec<Option<NaiveTime>> = (0..arr.len()).map(|idx|arr.value_as_time(idx)).collect();
            write_value(&v, &PostgresType::TIME_ARRAY, field_name, buf)
        }
        ArrowArray::Time64Nanosecond(arr) => {
            let v: Vec<Option<NaiveTime>> = (0..arr.len()).map(|idx|arr.value_as_time(idx)).collect();
            write_value(&v, &PostgresType::TIME_ARRAY, field_name, buf)
        }
        ArrowArray::DurationNanosecond(arr) => {
            let v: Vec<Option<PostgresDuration>> = (0..arr.len()).map(|idx|arr.value_as_duration(idx).map(|v| PostgresDuration { duration: v })).collect();
            write_value(&v, &PostgresType::INTERVAL_ARRAY, field_name, buf)
        }
        ArrowArray::DurationMicrosecond(arr) => {
            let v: Vec<Option<PostgresDuration>> = (0..arr.len()).map(|idx|arr.value_as_duration(idx).map(|v| PostgresDuration { duration: v })).collect();
            write_value(&v, &PostgresType::INTERVAL_ARRAY, field_name, buf)
        }
        ArrowArray::DurationMillisecond(arr) => {
            let v: Vec<Option<PostgresDuration>> = (0..arr.len()).map(|idx|arr.value_as_duration(idx).map(|v| PostgresDuration { duration: v })).collect();
            write_value(&v, &PostgresType::INTERVAL_ARRAY, field_name, buf)
        }
        ArrowArray::DurationSecond(arr) => {
            let v: Vec<Option<PostgresDuration>> = (0..arr.len()).map(|idx|arr.value_as_duration(idx).map(|v| PostgresDuration { duration: v })).collect();
            write_value(&v, &PostgresType::INTERVAL_ARRAY, field_name, buf)
        }
        ArrowArray::Binary(arr) => {
            let v: Vec<Option<&[u8]>> = arr.iter().collect();
            write_value(&v, &PostgresType::BYTEA_ARRAY, field_name, buf)
        }
        ArrowArray::LargeBinary(arr) => {
            let v: Vec<Option<&[u8]>> = arr.iter().collect();
            write_value(&v, &PostgresType::BYTEA_ARRAY, field_name, buf)
        }
        ArrowArray::String(arr) => {
            let v: Vec<Option<&str>> = arr.iter().collect();
            write_value(&v, &PostgresType::TEXT_ARRAY, field_name, buf)
        }
        ArrowArray::LargeStringArray(arr) => {
            let v: Vec<Option<&str>> = arr.iter().collect();
            write_value(&v, &PostgresType::TEXT_ARRAY, field_name, buf)
        }
        // we don't support any other array types
        // but would have already errored when we were processing the schema
        _ => unreachable!(),
    }
}

impl ArrowToPostgresBinaryEncoder {
    /// Creates a new writer which will write rows of the provided types to the provided sink.
    pub fn try_new(schema: Schema) -> Result<ArrowToPostgresBinaryEncoder, Error> {
        let fields = schema.fields.to_vec();

        Ok(ArrowToPostgresBinaryEncoder {
            fields,
            state: EncoderState::Created,
        })
    }

    pub fn write_header(&mut self, out: &mut BytesMut) {
        assert_eq!(self.state, EncoderState::Created);
        out.put(HEADER_MAGIC_BYTES);
        out.put_i32(0); // flags
        out.put_i32(0); // header extension
        self.state = EncoderState::Encoding;
    }

    pub fn write_batch(&mut self, batch: RecordBatch, buf: &mut BytesMut) -> Result<(), Error> {
        assert_eq!(self.state, EncoderState::Encoding);
        assert!(
            batch.num_columns() == self.fields.len(),
            "expected {} values but got {}",
            self.fields.len(),
            batch.num_columns(),
        );
        let n_rows = batch.num_rows();
        let n_cols = batch.num_columns();

        let columns = batch
            .columns()
            .iter()
            .zip(&self.fields)
            .map(|(col, field)| downcast_array(field.data_type(), col, field.name()))
            .collect::<Result<Vec<ArrowArray>, Error>>()?;

        for row in 0..n_rows {
            buf.put_i16(n_cols as i16);
            for (col, field) in columns.iter().zip(&self.fields) {
                let field_name = field.name();
                match col {
                    ArrowArray::Boolean(arr) => {
                        let v = check_null_mask(arr, row).map(|arr| arr.value(row));
                        write_value(&v, &PostgresType::BOOL, field_name, buf)?;
                    }
                    ArrowArray::Int8(arr) => {
                        write_value(
                            &value_from_primitive_array(arr, row).map(i16::from),
                            &PostgresType::INT2,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::Int16(arr) => {
                        write_value(
                            &value_from_primitive_array(arr, row),
                            &PostgresType::INT2,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::Int32(arr) => {
                        write_value(
                            &value_from_primitive_array(arr, row),
                            &PostgresType::INT4,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::Int64(arr) => {
                        write_value(
                            &value_from_primitive_array(arr, row),
                            &PostgresType::INT8,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::UInt8(arr) => {
                        write_value(
                            &value_from_primitive_array(arr, row).map(i16::from),
                            &PostgresType::INT2,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::UInt16(arr) => {
                        write_value(
                            &value_from_primitive_array(arr, row).map(i32::from),
                            &PostgresType::INT4,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::UInt32(arr) => {
                        write_value(
                            &value_from_primitive_array(arr, row).map(i64::from),
                            &PostgresType::INT8,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::Float16(arr) => {
                        write_value(
                            &value_from_primitive_array(arr, row).map(f32::from),
                            &PostgresType::FLOAT4,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::Float32(arr) => {
                        write_value(
                            &value_from_primitive_array(arr, row),
                            &PostgresType::FLOAT4,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::Float64(arr) => {
                        write_value(
                            &value_from_primitive_array(arr, row),
                            &PostgresType::FLOAT8,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::TimestampNanosecond(arr, true) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_datetime(row).unwrap());
                        let v = v.map(|v| Utc.from_utc_datetime(&v));
                        write_value(&v, &PostgresType::TIMESTAMPTZ, field_name, buf)?;
                    }
                    ArrowArray::TimestampMicrosecond(arr, true) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_datetime(row).unwrap());
                        let v = v.map(|v| Utc.from_utc_datetime(&v));
                        write_value(&v, &PostgresType::TIMESTAMPTZ, field_name, buf)?;
                    }
                    ArrowArray::TimestampMillisecond(arr, true) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_datetime(row).unwrap());
                        let v = v.map(|v| Utc.from_utc_datetime(&v));
                        write_value(&v, &PostgresType::TIMESTAMPTZ, field_name, buf)?;
                    }
                    ArrowArray::TimestampSecond(arr, true) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_datetime(row).unwrap());
                        let v = v.map(|v| Utc.from_utc_datetime(&v));
                        write_value(&v, &PostgresType::TIMESTAMPTZ, field_name, buf)?;
                    }
                    ArrowArray::TimestampNanosecond(arr, false) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_datetime(row).unwrap());
                        write_value(&v, &PostgresType::TIMESTAMP, field_name, buf)?;
                    }
                    ArrowArray::TimestampMicrosecond(arr, false) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_datetime(row).unwrap());
                        write_value(&v, &PostgresType::TIMESTAMP, field_name, buf)?;
                    }
                    ArrowArray::TimestampMillisecond(arr, false) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_datetime(row).unwrap());
                        write_value(&v, &PostgresType::TIMESTAMP, field_name, buf)?;
                    }
                    ArrowArray::TimestampSecond(arr, false) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_datetime(row).unwrap());
                        write_value(&v, &PostgresType::TIMESTAMP, field_name, buf)?;
                    }
                    ArrowArray::Date32(arr) => {
                        write_value(
                            &value_from_primitive_array(arr, row),
                            &PostgresType::DATE,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::Date64(arr) => {
                        write_value(
                            &value_from_primitive_array(arr, row),
                            &PostgresType::DATE,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::Time32Millisecond(arr) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_time(row).unwrap());
                        write_value(
                            &v,
                            &PostgresType::TIME,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::Time32Second(arr) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_time(row).unwrap());
                        write_value(
                            &v,
                            &PostgresType::TIME,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::Time64Nanosecond(arr) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_time(row).unwrap());
                        write_value(
                            &v,
                            &PostgresType::TIME,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::Time64Microsecond(arr) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_time(row).unwrap());
                        write_value(
                            &v,
                            &PostgresType::TIME,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::DurationNanosecond(arr) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_duration(row).unwrap());
                        write_value(
                            &v.map(|v| PostgresDuration { duration: v }),
                            &PostgresType::INTERVAL,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::DurationMicrosecond(arr) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_duration(row).unwrap());
                        write_value(
                            &v.map(|v| PostgresDuration { duration: v }),
                            &PostgresType::INTERVAL,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::DurationMillisecond(arr) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_duration(row).unwrap());
                        write_value(
                            &v.map(|v| PostgresDuration { duration: v }),
                            &PostgresType::INTERVAL,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::DurationSecond(arr) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_duration(row).unwrap());
                        write_value(
                            &v.map(|v| PostgresDuration { duration: v }),
                            &PostgresType::INTERVAL,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::Binary(arr) => {
                        write_value(
                            &check_null_mask(arr, row).map(|arr| arr.value(row)),
                            &PostgresType::BYTEA,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::LargeBinary(arr) => {
                        write_value(
                            &check_null_mask(arr, row).map(|arr| arr.value(row)),
                            &PostgresType::BYTEA,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::String(arr) => {
                        write_value(
                            &check_null_mask(arr, row).map(|arr| arr.value(row)),
                            &PostgresType::TEXT,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::LargeStringArray(arr) => {
                        write_value(
                            &check_null_mask(arr, row).map(|arr| arr.value(row)),
                            &PostgresType::TEXT,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::List(arr, inner_type) => {
                        let v = &check_null_mask(arr, row).map(|arr| arr.value(row));
                        match v {
                            Some(inner_arr) => {
                                let inner_arr =
                                    downcast_array(inner_type, inner_arr, field.name())?;
                                write_array(inner_arr, field_name, buf)?
                            }
                            None => write_null(buf),
                        };
                    }
                    ArrowArray::FixedSizeList(arr, inner_type) => {
                        let v = &check_null_mask(arr, row).map(|arr| arr.value(row));
                        match v {
                            Some(inner_arr) => {
                                let inner_arr =
                                    downcast_array(inner_type, inner_arr, field.name())?;
                                write_array(inner_arr, field_name, buf)?
                            }
                            None => write_null(buf),
                        };
                    }
                    ArrowArray::LargeList(arr, inner_type) => {
                        let v = &check_null_mask(arr, row).map(|arr| arr.value(row));
                        match v {
                            Some(inner_arr) => {
                                let inner_arr =
                                    downcast_array(inner_type, inner_arr, field.name())?;
                                write_array(inner_arr, field_name, buf)?
                            }
                            None => write_null(buf),
                        };
                    }
                };
            }
        }
        Ok(())
    }

    pub fn write_footer(&mut self, out: &mut BytesMut) -> Result<(), Error> {
        assert_eq!(self.state, EncoderState::Encoding);
        out.put_i16(-1);
        self.state = EncoderState::Finished;
        Ok(())
    }
}
