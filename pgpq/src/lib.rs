mod error;

use std::convert::TryFrom;
use std::io;

use crate::error::Error;
use arrow::array;
use arrow::array::as_primitive_array;
use arrow::datatypes::{DataType, Schema, TimeUnit};

use arrow::record_batch::RecordBatch;

use arrow_array::{types as array_types, Array, ArrayAccessor};

use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, BytesMut};
use chrono::{TimeZone, Utc};

use postgres_types::{to_sql_checked, IsNull, ToSql, Type};

const MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";

fn write_null(buf: &mut BytesMut) {
    let idx = buf.len();
    buf.put_i32(0);
    BigEndian::write_i32(&mut buf[idx..], -1);
}

#[inline]
fn write_value(
    v: &dyn ToSql,
    type_: &Type,
    field_name: &str,
    buf: &mut BytesMut,
) -> Result<(), Error> {
    let idx = buf.len();
    buf.put_i32(0);
    let len = match v
        .to_sql_checked(type_, buf)
        .map_err(|e| Error::to_sql(e, field_name))?
    {
        IsNull::Yes => -1,
        IsNull::No => i32::try_from(buf.len() - idx - 4)
            .map_err(|e| Error::encode(io::Error::new(io::ErrorKind::InvalidInput, e)))?,
    };
    BigEndian::write_i32(&mut buf[idx..], len);
    Ok(())
}

fn arrow_type_to_pg_type(tp: &DataType) -> Type {
    match tp {
        DataType::Null => {
            // Any value will do
            Type::INT2
        }
        DataType::Boolean => Type::BOOL,
        DataType::Int8 => Type::INT2,
        DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::UInt8 => Type::INT2,
        DataType::UInt16 => Type::INT4,
        DataType::UInt32 => Type::INT8,
        DataType::Float16 => Type::FLOAT4,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Timestamp(_, None) => Type::TIMESTAMP,
        DataType::Timestamp(_, Some(_)) => Type::TIMESTAMPTZ,
        DataType::Date32 => Type::DATE,
        DataType::Date64 => Type::DATE,
        DataType::Time32(_) => Type::TIME,
        DataType::Duration(_) => Type::INTERVAL,
        DataType::Binary => Type::BYTEA,
        DataType::FixedSizeBinary(_) => Type::BYTEA,
        DataType::LargeBinary => Type::BYTEA,
        DataType::Utf8 => Type::TEXT,
        DataType::LargeUtf8 => Type::TEXT,
        DataType::List(_) => {
            panic!("TODO, needs a lot of boilerplate")
        }
        DataType::FixedSizeList(_, _) => {
            panic!("TODO, needs a lot of boilerplate")
        }
        DataType::LargeList(_) => {
            panic!("TODO, needs a lot of boilerplate")
        }
        DataType::Struct(_) => {
            panic!("TODO, unpack as JSONB")
        }
        DataType::Dictionary(_, _) => {
            panic!("TODO, unpack as JSONB")
        }
        DataType::Decimal128(_, _) => {
            panic!("TODO, use https://docs.rs/rust_decimal/latest/rust_decimal/prelude/struct.Decimal.html#method.to_sql")
        }
        DataType::Decimal256(_, _) => {
            panic!("TODO, use https://docs.rs/rust_decimal/latest/rust_decimal/prelude/struct.Decimal.html#method.to_sql")
        }
        _ => panic!("UNSUPPORTED"),
    }
}

#[derive(Debug, Clone)]
struct PostgresField {
    type_: Type,
    name: String,
}

enum TimestampArray<'a> {
    Nanosecond(&'a array::TimestampNanosecondArray),
    Microsecond(&'a array::TimestampMicrosecondArray),
    Millisecond(&'a array::TimestampMillisecondArray),
    Second(&'a array::TimestampSecondArray),
}

impl<'a> TimestampArray<'a> {
    #[inline]
    pub fn value_as_datetime(&self, i: usize) -> Option<chrono::NaiveDateTime> {
        match self {
            Self::Nanosecond(arr) => {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value_as_datetime(i).expect("Invalid TIMESTAMP"))
                }
            }
            Self::Microsecond(arr) => {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value_as_datetime(i).expect("Invalid TIMESTAMP"))
                }
            }
            Self::Millisecond(arr) => {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value_as_datetime(i).expect("Invalid TIMESTAMP"))
                }
            }
            Self::Second(arr) => {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value_as_datetime(i).expect("Invalid TIMESTAMP"))
                }
            }
        }
    }
}

enum Time32Array<'a> {
    Millisecond(&'a array::Time32MillisecondArray),
    Second(&'a array::Time32SecondArray),
}

impl<'a> Time32Array<'a> {
    #[inline]
    pub fn value_as_time(&self, i: usize) -> Option<chrono::NaiveTime> {
        match self {
            Self::Millisecond(arr) => {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value_as_time(i).expect("Invalid TIME"))
                }
            }
            Self::Second(arr) => {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value_as_time(i).expect("Invalid TIME"))
                }
            }
        }
    }
}

enum Time64Array<'a> {
    Nanosecond(&'a array::Time64NanosecondArray),
    Microsecond(&'a array::Time64MicrosecondArray),
}

impl<'a> Time64Array<'a> {
    #[inline]
    pub fn value_as_time(&self, i: usize) -> Option<chrono::NaiveTime> {
        match self {
            Self::Nanosecond(arr) => {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value_as_time(i).expect("Invalid TIME"))
                }
            }
            Self::Microsecond(arr) => {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value_as_time(i).expect("Invalid TIME"))
                }
            }
        }
    }
}

#[derive(Debug)]
struct PostgresDuration {
    microseconds: i64,
}

impl ToSql for PostgresDuration {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<(dyn std::error::Error + Send + Sync)>> {
        out.put_i64(self.microseconds);
        Ok(IsNull::No)
    }
    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::INTERVAL)
    }

    to_sql_checked!();
}

#[inline]
fn get_value_checking_null_mask<T>(arr: &impl ArrayAccessor<Item = T>, index: usize) -> Option<T> {
    if arr.is_null(index) {
        None
    } else {
        Some(arr.value(index))
    }
}


#[derive(Debug, PartialEq)]
pub enum EncoderState {
    Created,
    Encoding,
    Finished,
}

#[derive(Debug)]
pub struct ArrowToPostgresBinaryEncoder {
    fields: Vec<PostgresField>,
    state: EncoderState,
}

impl ArrowToPostgresBinaryEncoder {
    /// Creates a new writer which will write rows of the provided types to the provided sink.
    pub fn new(schema: Schema) -> ArrowToPostgresBinaryEncoder {
        let pg_fields: Vec<PostgresField> = schema
            .fields
            .iter()
            .map(|f| PostgresField {
                type_: arrow_type_to_pg_type(f.data_type()),
                name: f.name().to_string(),
            })
            .collect();

        ArrowToPostgresBinaryEncoder {
            fields: pg_fields,
            state: EncoderState::Created,
        }
    }

    pub fn write_header(&mut self, out: &mut BytesMut) -> () {
        assert_eq!(self.state, EncoderState::Created);
        out.put(MAGIC);
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

        let columns = batch.columns();
        for row in 0..n_rows {
            buf.put_i16(n_cols as i16);
            for (col, arr_ref) in columns.iter().enumerate() {
                let field = &self.fields[col];
                let arr = &*arr_ref.clone();
                let field_name = &field.name;
                let pg_type = &field.type_;
                let mut write = |v: &dyn ToSql| {
                    write_value(v, pg_type, field_name, buf).unwrap();
                };
                match arr.data_type() {
                    DataType::Null => write_null(buf),
                    DataType::Boolean => {
                        let arr = array::as_boolean_array(arr);
                        let v = get_value_checking_null_mask(&arr, row);
                        write(&v)
                    }
                    DataType::Int8 => {
                        let arr = as_primitive_array::<array_types::Int8Type>(arr);
                        let v = get_value_checking_null_mask(&arr, row).map(i16::from);
                        write(&v)
                    }
                    DataType::Int16 => {
                        let arr = as_primitive_array::<array_types::Int16Type>(arr);
                        let v = get_value_checking_null_mask(&arr, row);
                        write(&v);
                    }
                    DataType::Int32 => {
                        let arr = as_primitive_array::<array_types::Int32Type>(arr);
                        let v = get_value_checking_null_mask(&arr, row);
                        write(&v);
                    }
                    DataType::Int64 => {
                        let arr = as_primitive_array::<array_types::Int64Type>(arr);
                        let v = get_value_checking_null_mask(&arr, row);
                        write(&v);
                    }
                    DataType::UInt8 => {
                        let arr = as_primitive_array::<array_types::UInt8Type>(arr);
                        let v = get_value_checking_null_mask(&arr, row).map(i16::from);
                        write(&v);
                    }
                    DataType::UInt16 => {
                        let arr = as_primitive_array::<array_types::UInt16Type>(arr);
                        let v = get_value_checking_null_mask(&arr, row).map(i32::from);
                        write(&v);
                    }
                    DataType::UInt32 => {
                        let arr = as_primitive_array::<array_types::UInt32Type>(arr);
                        let v = get_value_checking_null_mask(&arr, row).map(i64::from);
                        write(&v);
                    }
                    DataType::UInt64 => {
                        panic!("rust-postgres doesn't support u64 or i128")
                    }
                    DataType::Float16 => {
                        let arr = as_primitive_array::<array_types::Float16Type>(arr);
                        let v = get_value_checking_null_mask(&arr, row).map(f32::from);
                        write(&v);
                    }
                    DataType::Float32 => {
                        let arr = as_primitive_array::<array_types::Float32Type>(arr);
                        let v = get_value_checking_null_mask(&arr, row);
                        write(&v);
                    }
                    DataType::Float64 => {
                        let arr = as_primitive_array::<array_types::Float64Type>(arr);
                        let v = get_value_checking_null_mask(&arr, row);
                        write(&v);
                    }
                    DataType::Timestamp(time_unit, tz_option) => {
                        let arr = match time_unit {
                            TimeUnit::Nanosecond => {
                                TimestampArray::Nanosecond(as_primitive_array::<
                                    array_types::TimestampNanosecondType,
                                >(arr))
                            }
                            TimeUnit::Microsecond => {
                                TimestampArray::Microsecond(as_primitive_array::<
                                    array_types::TimestampMicrosecondType,
                                >(arr))
                            }
                            TimeUnit::Millisecond => {
                                TimestampArray::Millisecond(as_primitive_array::<
                                    array_types::TimestampMillisecondType,
                                >(arr))
                            }
                            TimeUnit::Second => {
                                TimestampArray::Second(as_primitive_array::<
                                    array_types::TimestampSecondType,
                                >(arr))
                            }
                        };
                        match tz_option {
                            Some(_) => {
                                let v = arr
                                    .value_as_datetime(row)
                                    .map(|v| Utc.from_local_datetime(&v).unwrap());
                                write(&v)
                            }
                            None => {
                                let v = arr.value_as_datetime(row);
                                write(&v)
                            }
                        }
                    }
                    DataType::Date32 => {
                        let arr = as_primitive_array::<array_types::Date32Type>(arr);
                        let v = get_value_checking_null_mask(&arr, row);
                        write(&v);
                    }
                    DataType::Date64 => {
                        let arr = as_primitive_array::<array_types::Date64Type>(arr);
                        let v = get_value_checking_null_mask(&arr, row);
                        write(&v);
                    }
                    DataType::Time32(time_unit) => {
                        let arr = match time_unit {
                            TimeUnit::Millisecond => {
                                Time32Array::Millisecond(as_primitive_array::<
                                    array_types::Time32MillisecondType,
                                >(arr))
                            }
                            TimeUnit::Second => {
                                Time32Array::Second(as_primitive_array::<
                                    array_types::Time32SecondType,
                                >(arr))
                            }
                            _ => panic!("Time32 does not support {time_unit:?}"),
                        };
                        let v = arr.value_as_time(row);
                        write(&v);
                    }
                    DataType::Time64(time_unit) => {
                        let arr = match time_unit {
                            TimeUnit::Nanosecond => {
                                Time64Array::Nanosecond(as_primitive_array::<
                                    array_types::Time64NanosecondType,
                                >(arr))
                            }
                            TimeUnit::Microsecond => {
                                Time64Array::Microsecond(as_primitive_array::<
                                    array_types::Time64MicrosecondType,
                                >(arr))
                            }
                            _ => panic!("Time64 does not support {time_unit:?}"),
                        };
                        for row in 0..n_rows {
                            let v = arr.value_as_time(row);
                            write(&v)
                        }
                    }
                    DataType::Duration(time_unit) => match time_unit {
                        TimeUnit::Microsecond => {
                            let arr =
                                as_primitive_array::<array_types::DurationMicrosecondType>(arr);
                            match get_value_checking_null_mask(&arr, row) {
                                Some(v) => write(&PostgresDuration { microseconds: v }),
                                none => write(&none),
                            }
                        }
                        time_unit => {
                            panic!("Durations in units of {time_unit:?} are not supported")
                        }
                    },
                    DataType::Binary => {
                        let arr = arr.as_any().downcast_ref::<array::BinaryArray>().unwrap();
                        let v = get_value_checking_null_mask(&arr, row);
                        write(&v)
                    }
                    DataType::FixedSizeBinary(_) => {
                        let arr = arr
                            .as_any()
                            .downcast_ref::<array::FixedSizeBinaryArray>()
                            .unwrap();
                        let v = get_value_checking_null_mask(&arr, row);
                        write(&v)
                    }
                    DataType::LargeBinary => {
                        let arr = arr
                            .as_any()
                            .downcast_ref::<array::LargeBinaryArray>()
                            .unwrap();
                        let v = get_value_checking_null_mask(&arr, row);
                        write(&v)
                    }
                    DataType::Utf8 => {
                        let arr = array::as_string_array(arr);
                        let v = get_value_checking_null_mask(&arr, row);
                        write(&v)
                    }
                    DataType::LargeUtf8 => {
                        let arr = array::as_largestring_array(arr);
                        let v = get_value_checking_null_mask(&arr, row);
                        write(&v)
                    }
                    _ => panic!("Unsupported data type"),
                }
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
    use arrow::array;
    use arrow::array::{make_array, ArrayRef};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::RecordBatchReader;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use rstest::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn run_snap_test(name: &str, batch: RecordBatch, schema: Schema) {
        let mut buff = BytesMut::new();
        let mut writer = ArrowToPostgresBinaryEncoder::new(schema);

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
            assert_eq!(existing, &buff[..])
        }
    }

    #[derive(Debug)]
    struct TestField {
        data_type: DataType,
        nullable: bool,
        data: ArrayRef,
    }

    impl TestField {
        fn new(data_type: DataType, nullable: bool, data: impl array::Array) -> Self {
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

    #[rstest]
    #[case::int8_non_nullable("int8_non_nullable", TestField::new(DataType::Int8, false, array::Int8Array::from(vec![-1, 0, 1])))]
    #[case::int64_non_nullable("int64_non_nullable", TestField::new(DataType::Int64, false, array::Int64Array::from(vec![-1, 0, 1])))]
    #[case::int8_nullable("int8_nullable", TestField::new(DataType::Int8, true, array::Int8Array::from(vec![-1, 0, 1])))]
    #[case::int64_nullable("int64_nullable", TestField::new(DataType::Int64, true, array::Int64Array::from(vec![-1, 0, 1])))]
    #[case::int8_nullable_nulls("int8_nullable_nulls", TestField::new(DataType::Int8, true, array::Int8Array::from(vec![Some(-1), Some(0), None])))]
    #[case::int64_nullable_nulls("int64_nullable_nulls", TestField::new(DataType::Int64, true, array::Int64Array::from(vec![Some(-1), Some(0), None])))]
    #[case::timestamp_second_with_tz_non_nullable("timestamp_second_with_tz_non_nullable", TestField::new(DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())), false, array::TimestampSecondArray::from(vec![
        0,
        1675210660,
        1675210661,
    ]).with_timezone("America/New_York".to_string())))]
    #[trace] //This attribute enable traceing
    fn test_field_types(#[case] name: &str, #[case] input: TestField) {
        let (schema, batch) = create_batch_from_fields(vec![input]);
        run_snap_test(name, batch, schema);
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
