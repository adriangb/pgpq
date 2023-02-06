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
            write_value(&v, &PostgresType::INT4_ARRAY, field_name, buf)
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
                            &PostgresType::INT4,
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
                            &PostgresType::INT4,
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
                            &value_from_primitive_array(arr, row),
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
                        write_value(
                            &value_from_primitive_array(arr, row),
                            &PostgresType::TIME,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::Time32Second(arr) => {
                        write_value(
                            &value_from_primitive_array(arr, row),
                            &PostgresType::TIME,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::Time64Nanosecond(arr) => {
                        write_value(
                            &value_from_primitive_array(arr, row),
                            &PostgresType::TIME,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::Time64Microsecond(arr) => {
                        write_value(
                            &value_from_primitive_array(arr, row),
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
                            &PostgresType::TIMESTAMPTZ,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::DurationMicrosecond(arr) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_duration(row).unwrap());
                        write_value(
                            &v.map(|v| PostgresDuration { duration: v }),
                            &PostgresType::TIMESTAMPTZ,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::DurationMillisecond(arr) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_duration(row).unwrap());
                        write_value(
                            &v.map(|v| PostgresDuration { duration: v }),
                            &PostgresType::TIMESTAMPTZ,
                            field_name,
                            buf,
                        )?;
                    }
                    ArrowArray::DurationSecond(arr) => {
                        let v = &check_null_mask(arr, row)
                            .map(|arr| arr.value_as_duration(row).unwrap());
                        write_value(
                            &v.map(|v| PostgresDuration { duration: v }),
                            &PostgresType::TIMESTAMPTZ,
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

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{make_array, ArrayRef};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use arrow_array::RecordBatchReader;
    use arrow_array::{builder as array_builder, types as array_types, GenericListArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use rstest::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::thread::current;

    fn run_snap_test(name: &str, batch: RecordBatch, schema: Schema) {
        let mut buff = BytesMut::new();
        let mut writer = ArrowToPostgresBinaryEncoder::try_new(schema).unwrap();

        writer.write_header(&mut buff);
        writer.write_batch(batch, &mut buff).unwrap();
        writer.write_footer(&mut buff).unwrap();

        let snap_file =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("src/snapshots/{name}.bin"));
        if !snap_file.exists() {
            fs::write(snap_file.clone(), &buff[..]).unwrap();
            panic!("wrote new snap at {snap_file:?}")
        } else {
            let existing = fs::read(snap_file).unwrap();
            assert_eq!(
                existing,
                &buff[..],
                "values did not match. First 50 bytes: {:?} (actual) vs {:?} (expected)",
                &existing[..50],
                &buff[..50]
            )
        }
    }

    #[derive(Debug)]
    struct TestField {
        data_type: DataType,
        nullable: bool,
        data: ArrayRef,
    }

    impl TestField {
        fn new(data_type: DataType, nullable: bool, data: impl arrow_array::Array) -> Self {
            TestField {
                data_type,
                nullable,
                data: make_array(data.data().clone()),
            }
        }
    }

    fn create_batch_from_fields(fields: Vec<TestField>) -> (Schema, RecordBatch) {
        let schema = Schema::new(
            fields
                .iter()
                .enumerate()
                .map(|(i, f)| Field::new(format!("col_{i}"), f.data_type.clone(), f.nullable))
                .collect(),
        );
        let columns = fields.iter().map(|f| f.data.clone()).collect();
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns).unwrap();

        (schema, batch)
    }

    fn list_array_from_iter_primitive<T, P, I>(
        iter: I,
        nullable: bool,
        item_nullable: bool,
    ) -> GenericListArray<i32>
    where
        T: ArrowPrimitiveType,
        P: IntoIterator<Item = Option<<T as ArrowPrimitiveType>::Native>>,
        I: IntoIterator<Item = Option<P>>,
    {
        let iter = iter.into_iter();
        let size_hint = iter.size_hint().0;
        let mut builder = array_builder::GenericListBuilder::with_capacity(
            array_builder::PrimitiveBuilder::<T>::new(),
            size_hint,
        );

        // TODO: how can we set nullability and inner item nullability on the generated array? It seems to be hardcoded to `true`

        for outer in iter {
            match outer {
                Some(inner) => {
                    for inner_item in inner {
                        match item_nullable {
                            true => builder.values().append_option(inner_item),
                            false => match inner_item {
                                Some(v) => builder.values().append_value(v),
                                None => {
                                    panic!("Expected non-nullable inner items but got a None value")
                                }
                            },
                        }
                    }
                    builder.append(true);
                }
                None => {
                    if !nullable {
                        panic!("Expected non nullable list items but got a None")
                    }
                    builder.append(true)
                }
            }
        }
        builder.finish()
    }

    #[derive(Debug)]
    struct TestCase {
        description: String,
    }

    #[fixture]
    fn testcase() -> TestCase {
        let name = current().name().unwrap().to_string();
        let description = name
            .split("::")
            .map(|item| {
                item.split('_')
                    .skip(2)
                    .collect::<Vec<&str>>()
                    .join("_")

            })
            .last()
            .unwrap();
        TestCase { description }
    }

    #[rstest]
    #[case::int8_non_nullable(TestField::new(DataType::Int8, false, arrow_array::Int8Array::from(vec![-1, 0, 1])))]
    #[case::int64_non_nullable(TestField::new(DataType::Int64, false, arrow_array::Int64Array::from(vec![-1, 0, 1])))]
    #[case::int8_nullable(TestField::new(DataType::Int8, true, arrow_array::Int8Array::from(vec![-1, 0, 1])))]
    #[case::int64_nullable(TestField::new(DataType::Int64, true, arrow_array::Int64Array::from(vec![-1, 0, 1])))]
    #[case::int8_nullable_nulls(TestField::new(DataType::Int8, true, arrow_array::Int8Array::from(vec![Some(-1), Some(0), None])))]
    #[case::int64_nullable_nulls(TestField::new(DataType::Int64, true, arrow_array::Int64Array::from(vec![Some(-1), Some(0), None])))]
    #[case::timestamp_second_with_tz_non_nullable(
        TestField::new(
            DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
            false,
            arrow_array::TimestampSecondArray::from(vec![0, 1675210660, 1675210661]).with_timezone("America/New_York".to_string())
        )
    )]
    #[case::i32_nullable_list_nullable_items(
        TestField::new(
            DataType::List(Box::new(Field::new("item".to_string(), DataType::Int32, true))),
            true,
            list_array_from_iter_primitive::<array_types::Int32Type, _, _>(vec![Some(vec![Some(123), None]), None], true, true)
        )
    )]
    #[trace] //This attribute enable tracing
    fn test_field_types(testcase: TestCase, #[case] input: TestField) {
        let (schema, batch) = create_batch_from_fields(vec![input]);
        run_snap_test(&testcase.description, batch, schema);
    }

    #[test]
    fn test_example_data() {
        let file = fs::File::open(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("testdata/yellow_tripdata_2022-01.parquet"),
        )
        .unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.build().unwrap();
        let record_batch = reader.next().unwrap().unwrap();
        run_snap_test(
            "yellow_cab_tripdata",
            record_batch,
            Schema::new(reader.schema().fields().clone()),
        )
    }
}
